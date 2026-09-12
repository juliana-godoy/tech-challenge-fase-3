"""AWS Glue Job: Bronze -> Silver -> Gold -> Glue Data Catalog.

Entradas: três CSVs originais em s3://<bucket>/data-input/bronze/.
Saídas: Parquet Snappy em data-output/silver e data-output/gold, tabelas
externas no Glue Data Catalog e auditoria em log/pipeline/auditoria.
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from mappings import DATABASE_NAME, GOLD_TABLE_PATHS, SOURCE_SPECS
from quality import assert_gold_non_empty, build_audit_frame, validate_silver
from transformations import build_silver


BUCKET = "lab-255375488094"


def read_bronze_csv(glue_context: GlueContext, uri: str) -> DataFrame:
    return (
        glue_context.spark_session.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(uri)
    )


def safe_ratio(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.when(denominator > 0, F.round(numerator / denominator * 100, 2)).otherwise(
        F.lit(None).cast("double")
    )


def build_market(silver: DataFrame) -> DataFrame:
    base = silver.filter(F.col("cargo_padronizado").isNotNull())
    totals = base.groupBy("ano_pesquisa").agg(F.count("*").alias("denominador_ano"))
    return (
        base.groupBy("ano_pesquisa", "cargo_padronizado", "senioridade")
        .agg(F.count("*").alias("respondentes"))
        .join(totals, "ano_pesquisa")
        .withColumn("percentual_ano", safe_ratio(F.col("respondentes"), F.col("denominador_ano")))
    )


def remuneration_segment(silver: DataFrame, dimension_type: str, dimension: str) -> DataFrame:
    return (
        silver.filter(F.col(dimension).isNotNull())
        .groupBy("ano_pesquisa", F.col(dimension).alias("dimensao_valor"))
        .agg(
            F.count("salario_ordem").alias("respondentes_faixa_valida"),
            F.count("salario_ponto_medio").alias("respondentes_ponto_medio"),
            F.round(F.avg("salario_ponto_medio"), 2).alias("salario_medio_aproximado"),
            F.expr("percentile_approx(salario_ordem, 0.5)").cast("int").alias("faixa_mediana_ordem"),
        )
        .withColumn("dimensao_tipo", F.lit(dimension_type))
        .select(
            "ano_pesquisa",
            "dimensao_tipo",
            "dimensao_valor",
            "respondentes_faixa_valida",
            "respondentes_ponto_medio",
            "salario_medio_aproximado",
            "faixa_mediana_ordem",
        )
    )


def build_remuneration(silver: DataFrame) -> DataFrame:
    dimensions = [
        ("cargo", "cargo_padronizado"),
        ("senioridade", "senioridade"),
        ("regiao", "regiao_moradia"),
        ("modelo_trabalho", "modelo_trabalho_padronizado"),
        ("genero", "genero"),
        ("experiencia", "tempo_experiencia_historica"),
    ]
    frames = [remuneration_segment(silver, label, column) for label, column in dimensions]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def build_diversity(silver: DataFrame) -> DataFrame:
    base = silver.filter(F.col("genero").isNotNull() & F.col("cargo_padronizado").isNotNull())
    denominator = Window.partitionBy("ano_pesquisa", "cargo_padronizado", "senioridade")
    return (
        base.groupBy("ano_pesquisa", "genero", "cargo_padronizado", "senioridade")
        .agg(
            F.count("*").alias("respondentes"),
            F.count("salario_ordem").alias("respondentes_com_salario"),
            F.round(F.avg("salario_ponto_medio"), 2).alias("salario_medio_aproximado"),
            F.sum(F.when(F.col("eh_gestor") == 1, 1).otherwise(0)).alias("gestores"),
        )
        .withColumn("denominador_segmento", F.sum("respondentes").over(denominator))
        .withColumn("percentual_segmento", safe_ratio(F.col("respondentes"), F.col("denominador_segmento")))
    )


def technology_long(silver: DataFrame) -> DataFrame:
    return silver.select(
        "ano_pesquisa",
        "cargo_padronizado",
        "senioridade",
        F.expr(
            "stack(8, "
            "'SQL', usa_sql, "
            "'Python', usa_python, "
            "'AWS', usa_aws, "
            "'Azure', usa_azure, "
            "'GCP', usa_gcp, "
            "'Databricks', usa_databricks, "
            "'Power BI', usa_power_bi, "
            "'Tableau', usa_tableau) as (tecnologia, adotou)"
        ),
    )


def technology_segment(long_frame: DataFrame, segment_type: str, segment_column: str | None) -> DataFrame:
    if segment_column:
        frame = long_frame.withColumn("segmento_valor", F.col(segment_column)).filter(
            F.col(segment_column).isNotNull()
        )
    else:
        frame = long_frame.withColumn("segmento_valor", F.lit("Todos"))
    return (
        frame.groupBy("ano_pesquisa", "tecnologia", "segmento_valor")
        .agg(
            F.count("adotou").alias("respondentes_elegiveis"),
            F.sum(F.coalesce(F.col("adotou"), F.lit(0))).cast("long").alias("adotantes"),
        )
        .withColumn("percentual_adocao", safe_ratio(F.col("adotantes"), F.col("respondentes_elegiveis")))
        .withColumn("segmento_tipo", F.lit(segment_type))
        .select(
            "ano_pesquisa",
            "tecnologia",
            "segmento_tipo",
            "segmento_valor",
            "respondentes_elegiveis",
            "adotantes",
            "percentual_adocao",
        )
    )


def build_technologies(silver: DataFrame) -> DataFrame:
    long_frame = technology_long(silver)
    return (
        technology_segment(long_frame, "geral", None)
        .unionByName(technology_segment(long_frame, "cargo", "cargo_padronizado"))
        .unionByName(technology_segment(long_frame, "senioridade", "senioridade"))
    )


def ai_segment(silver: DataFrame, segment_type: str, segment_column: str | None) -> DataFrame:
    base = silver.filter((F.col("ano_pesquisa") >= 2023) & F.col("usa_ia_generativa").isNotNull())
    if segment_column:
        base = base.withColumn("segmento_valor", F.col(segment_column)).filter(
            F.col(segment_column).isNotNull()
        )
    else:
        base = base.withColumn("segmento_valor", F.lit("Todos"))
    return (
        base.groupBy("ano_pesquisa", "segmento_valor")
        .agg(
            F.count("usa_ia_generativa").alias("respondentes_validos"),
            F.sum("usa_ia_generativa").cast("long").alias("adotantes_ia"),
            F.sum(F.coalesce(F.col("ia_gratuita"), F.lit(0))).cast("long").alias("uso_gratuito"),
            F.sum(F.coalesce(F.col("ia_paga_propria"), F.lit(0))).cast("long").alias("uso_pago_proprio"),
            F.sum(F.coalesce(F.col("ia_paga_empresa"), F.lit(0))).cast("long").alias("uso_pago_empresa"),
            F.sum(F.coalesce(F.col("ia_copilot"), F.lit(0))).cast("long").alias("uso_copilot"),
        )
        .withColumn("percentual_adocao_ia", safe_ratio(F.col("adotantes_ia"), F.col("respondentes_validos")))
        .withColumn("segmento_tipo", F.lit(segment_type))
        .select(
            "ano_pesquisa",
            "segmento_tipo",
            "segmento_valor",
            "respondentes_validos",
            "adotantes_ia",
            "percentual_adocao_ia",
            "uso_gratuito",
            "uso_pago_proprio",
            "uso_pago_empresa",
            "uso_copilot",
        )
    )


def build_ai(silver: DataFrame) -> DataFrame:
    dimensions = [
        ("geral", None),
        ("cargo", "cargo_padronizado"),
        ("senioridade", "senioridade"),
        ("regiao", "regiao_moradia"),
        ("modelo_trabalho", "modelo_trabalho_padronizado"),
        ("faixa_salarial", "faixa_salarial_original"),
    ]
    frames = [ai_segment(silver, label, column) for label, column in dimensions]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def build_region_work(silver: DataFrame) -> DataFrame:
    return (
        silver.filter(F.col("regiao_moradia").isNotNull())
        .groupBy("ano_pesquisa", "regiao_moradia", "modelo_trabalho_padronizado", "senioridade")
        .agg(
            F.count("*").alias("respondentes"),
            F.count("salario_ponto_medio").alias("respondentes_ponto_medio"),
            F.round(F.avg("salario_ponto_medio"), 2).alias("salario_medio_aproximado"),
            F.sum(F.when(F.col("usa_ia_generativa") == 1, 1).otherwise(0)).alias("adotantes_ia"),
            F.count("usa_ia_generativa").alias("respondentes_validos_ia"),
        )
        .withColumn(
            "percentual_adocao_ia",
            safe_ratio(F.col("adotantes_ia"), F.col("respondentes_validos_ia")),
        )
    )


def build_kpis(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("ano_pesquisa")
        .agg(
            F.count("*").alias("total_respondentes"),
            F.count("genero").alias("genero_respondentes"),
            F.sum(F.when(F.col("genero") == "Feminino", 1).otherwise(0)).alias("mulheres"),
            F.count("senioridade").alias("senioridade_respondentes"),
            F.sum(F.when(F.col("senioridade") == "Sênior", 1).otherwise(0)).alias("profissionais_seniores"),
            F.count("modelo_trabalho_padronizado").alias("modelo_trabalho_respondentes"),
            F.sum(F.when(F.col("modelo_trabalho_padronizado") == "Remoto", 1).otherwise(0)).alias("remotos"),
            F.count("usa_sql").alias("sql_elegiveis"),
            F.sum(F.coalesce(F.col("usa_sql"), F.lit(0))).alias("sql_adotantes"),
            F.count("usa_python").alias("python_elegiveis"),
            F.sum(F.coalesce(F.col("usa_python"), F.lit(0))).alias("python_adotantes"),
            F.count("usa_ia_generativa").alias("ia_elegiveis_validos"),
            F.sum(F.coalesce(F.col("usa_ia_generativa"), F.lit(0))).alias("ia_adotantes"),
            F.sum("flag_ia_contraditoria").alias("ia_respostas_contraditorias"),
            F.sum("quantidade_flags_qualidade").alias("flags_qualidade"),
        )
        .withColumn("percentual_mulheres", safe_ratio(F.col("mulheres"), F.col("genero_respondentes")))
        .withColumn(
            "percentual_seniores",
            safe_ratio(F.col("profissionais_seniores"), F.col("senioridade_respondentes")),
        )
        .withColumn("percentual_remoto", safe_ratio(F.col("remotos"), F.col("modelo_trabalho_respondentes")))
        .withColumn("percentual_sql", safe_ratio(F.col("sql_adotantes"), F.col("sql_elegiveis")))
        .withColumn("percentual_python", safe_ratio(F.col("python_adotantes"), F.col("python_elegiveis")))
        .withColumn("percentual_ia", safe_ratio(F.col("ia_adotantes"), F.col("ia_elegiveis_validos")))
    )


def write_catalog_table(frame: DataFrame, table_name: str, s3_uri: str) -> None:
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .option("path", s3_uri)
        .saveAsTable(f"{DATABASE_NAME}.{table_name}")
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} LOCATION 's3://{BUCKET}/data-output/'")

    silver_years = []
    audits = []
    processed_at = F.current_timestamp()
    for year, spec in SOURCE_SPECS.items():
        uri = f"s3://{BUCKET}/{spec['bronze_key']}"
        print(f"Lendo Bronze {year}: {uri}")
        bronze = read_bronze_csv(glue_context, uri)
        if len(bronze.columns) != {2022: 353, 2023: 399, 2024: 403}[year]:
            raise AssertionError(f"Quantidade de colunas inesperada em {year}: {len(bronze.columns)}")
        silver_year = build_silver(bronze, year, processed_at).cache()
        audits.append(validate_silver(silver_year, year))
        silver_years.append(silver_year)

    silver = silver_years[0]
    for frame in silver_years[1:]:
        silver = silver.unionByName(frame)
    silver = silver.cache()

    gold_tables = {
        "gold_kpis_anuais": build_kpis(silver),
        "gold_mercado_profissionais": build_market(silver),
        "gold_remuneracao": build_remuneration(silver),
        "gold_diversidade": build_diversity(silver),
        "gold_tecnologias": build_technologies(silver),
        "gold_inteligencia_artificial": build_ai(silver),
        "gold_regiao_trabalho": build_region_work(silver),
    }
    assert_gold_non_empty(gold_tables)
    audit_frame = build_audit_frame(spark, audits)

    write_catalog_table(
        silver,
        "silver_profissionais",
        f"s3://{BUCKET}/{GOLD_TABLE_PATHS['silver_profissionais']}",
    )
    for table_name, frame in gold_tables.items():
        write_catalog_table(frame, table_name, f"s3://{BUCKET}/{GOLD_TABLE_PATHS[table_name]}")
    write_catalog_table(
        audit_frame,
        "auditoria_pipeline",
        f"s3://{BUCKET}/{GOLD_TABLE_PATHS['auditoria_pipeline']}",
    )

    print("KPIs anuais produzidos:")
    gold_tables["gold_kpis_anuais"].orderBy("ano_pesquisa").show(truncate=False)
    print(f"Catálogo criado/atualizado: {DATABASE_NAME}")
    job.commit()


if __name__ == "__main__":
    main()
