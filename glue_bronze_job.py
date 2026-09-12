"""AWS Glue job: valida os CSVs originais armazenados na camada Bronze."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from uuid import uuid4

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BUCKET = "lab-255375488094"
BRONZE_BASE = f"s3://{BUCKET}/data-input/bronze"
AUDIT_BASE = f"s3://{BUCKET}/log/bronze/validacao"
FONTES = {
    2022: {
        "arquivo": "State_of_data_2022.csv",
        "linhas": 4271,
        "colunas": 353,
        "duplicados": 1,
    },
    2023: {
        "arquivo": "State_of_data_BR_2023_Kaggle - df_survey_2023.csv",
        "linhas": 5293,
        "colunas": 399,
        "duplicados": 0,
    },
    2024: {
        "arquivo": "Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv",
        "linhas": 5217,
        "colunas": 403,
        "duplicados": 2,
    },
}


def spark_col(nome: str):
    return F.col(f"`{nome.replace('`', '``')}`")


def ler_csv_bruto(spark, caminho: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(caminho)
    )


def serializar_linha(df: DataFrame):
    return F.to_json(
        F.struct(*[spark_col(nome).alias(nome) for nome in df.columns]),
        {"ignoreNullFields": "false"},
    )


def medir_duplicados_excedentes(df: DataFrame) -> int:
    total = (
        df.select(serializar_linha(df).alias("_linha_json"))
        .groupBy("_linha_json")
        .count()
        .filter(F.col("count") > 1)
        .select(F.coalesce(F.sum(F.col("count") - F.lit(1)), F.lit(0)).alias("total"))
        .first()["total"]
    )
    return int(total)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    id_execucao = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    data_processamento = datetime.now(timezone.utc).replace(tzinfo=None)
    resultados = []

    for ano in sorted(FONTES):
        esperado = FONTES[ano]
        arquivo = esperado["arquivo"]
        caminho = f"{BRONZE_BASE}/{ano}-dataset/{arquivo}"
        print(f"Validando {ano}: {caminho}")

        df = ler_csv_bruto(spark, caminho).cache()
        linhas = df.count()
        colunas = len(df.columns)
        duplicados = medir_duplicados_excedentes(df)
        assert linhas == esperado["linhas"], (ano, "linhas", linhas, esperado["linhas"])
        assert colunas == esperado["colunas"], (ano, "colunas", colunas, esperado["colunas"])
        assert duplicados == esperado["duplicados"], (
            ano,
            "duplicados",
            duplicados,
            esperado["duplicados"],
        )

        resultados.append(
            {
                "ano_pesquisa": ano,
                "arquivo_origem": arquivo,
                "caminho_bronze": caminho,
                "camada_origem": "bronze",
                "registros": linhas,
                "colunas": colunas,
                "duplicados_excedentes": duplicados,
                "status": "OK",
            }
        )
        df.unpersist()

    auditoria = (
        spark.createDataFrame(resultados)
        .withColumn("id_execucao", F.lit(id_execucao))
        .withColumn(
            "data_hora_processamento",
            F.lit(data_processamento).cast("timestamp"),
        )
    )
    caminho_auditoria = f"{AUDIT_BASE}/id_execucao={id_execucao}/"
    auditoria.coalesce(1).write.mode("overwrite").parquet(caminho_auditoria)
    auditoria.orderBy("ano_pesquisa").show(truncate=False)
    print(f"Auditoria gravada em: {caminho_auditoria}")

    job.commit()


if __name__ == "__main__":
    main()
