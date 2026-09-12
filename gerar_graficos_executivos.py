"""Gera gráficos executivos a partir dos artefatos locais ou do Athena.

Uso local:
    python src/gerar_graficos_executivos.py --source local

Uso como AWS Glue Python Shell job:
    --source athena
    --bucket lab-255375488094
    --database fiap_tech_challenge_fase3
    --athena-output s3://lab-255375488094/data-output/athena-results/
    --s3-output s3://lab-255375488094/data-output/graficos-executivos/
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter, PercentFormatter


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_DIR / "output" / "graficos_executivos"
BLUE = "#2F80ED"
NAVY = "#12233F"
CYAN = "#56CCF2"
GREEN = "#27AE60"
ORANGE = "#F2994A"
PURPLE = "#9B51E0"
RED = "#EB5757"
GRAY = "#667085"
LIGHT_GRAY = "#E4E7EC"
PALETTE = [BLUE, CYAN, GREEN, ORANGE, PURPLE, RED, GRAY]


def configure_style() -> None:
    plt.rcParams.update(
        {
            "figure.figsize": (12, 6.75),
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.titleweight": "bold",
            "axes.titlesize": 18,
            "axes.titlepad": 18,
            "axes.labelsize": 11,
            "axes.edgecolor": LIGHT_GRAY,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.color": LIGHT_GRAY,
            "grid.linewidth": 0.8,
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "legend.frameon": False,
            "savefig.facecolor": "white",
            "savefig.bbox": "tight",
        }
    )


def br_number(value: float, decimals: int = 1) -> str:
    text = f"{value:,.{decimals}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def money(value: float) -> str:
    return f"R$ {br_number(value / 1000, 1)} mil"


def finish_figure(fig: plt.Figure, output_dir: Path, file_name: str, source: str) -> Path:
    fig.text(0.01, 0.01, source, color=GRAY, fontsize=8)
    fig.tight_layout(rect=(0, 0.045, 1, 1))
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / file_name
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    frame = frame.copy()
    for column in columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


class AthenaRunner:
    def __init__(self, region: str, database: str, output: str, workgroup: str) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9_]+", database):
            raise ValueError("Nome de database inválido")
        import boto3

        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.database = database
        self.output = output.rstrip("/") + "/"
        self.workgroup = workgroup
        self.executions: list[dict[str, str]] = []

    @staticmethod
    def split_s3_uri(uri: str) -> tuple[str, str]:
        match = re.fullmatch(r"s3://([^/]+)/(.+)", uri)
        if not match:
            raise ValueError(f"URI S3 inválida: {uri}")
        return match.group(1), match.group(2)

    def query(self, name: str, sql: str, timeout_seconds: int = 300) -> pd.DataFrame:
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database, "Catalog": "AwsDataCatalog"},
            ResultConfiguration={"OutputLocation": self.output},
            WorkGroup=self.workgroup,
        )
        query_id = response["QueryExecutionId"]
        deadline = time.time() + timeout_seconds
        while True:
            execution = self.athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in {"FAILED", "CANCELLED"}:
                reason = execution["Status"].get("StateChangeReason", "sem detalhe")
                raise RuntimeError(f"Consulta {name} terminou como {state}: {reason}")
            if time.time() >= deadline:
                raise TimeoutError(f"Consulta {name} excedeu {timeout_seconds}s")
            time.sleep(2)

        output_location = execution["ResultConfiguration"]["OutputLocation"]
        bucket, key = self.split_s3_uri(output_location)
        body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        self.executions.append({"name": name, "query_execution_id": query_id})
        return pd.read_csv(io.BytesIO(body))


def load_local(project_dir: Path) -> dict[str, pd.DataFrame]:
    analytics = project_dir / "artifacts" / "analytics"
    files = {
        "kpis": "kpis_anuais.csv",
        "diversidade": "diversidade_genero.csv",
        "mercado": "mercado_cargos.csv",
        "remuneracao_cargo": "remuneracao_cargo_2024.csv",
        "remuneracao_senioridade": "remuneracao_senioridade.csv",
        "tecnologias": "tecnologias_geral.csv",
        "ia_geral": "ia_geral.csv",
        "trabalho": "modelos_trabalho.csv",
    }
    return {name: pd.read_csv(analytics / file_name) for name, file_name in files.items()}


def load_athena(runner: AthenaRunner) -> dict[str, pd.DataFrame]:
    db = runner.database
    table = lambda name: f'"{db}"."{name}"'
    queries = {
        "kpis": f"SELECT * FROM {table('gold_kpis_anuais')} ORDER BY ano_pesquisa",
        "diversidade": f"""
            SELECT ano_pesquisa, genero, SUM(respondentes) AS respondentes
            FROM {table('gold_diversidade')}
            GROUP BY ano_pesquisa, genero
            ORDER BY ano_pesquisa, genero
        """,
        "mercado": f"""
            SELECT ano_pesquisa, cargo_padronizado, SUM(respondentes) AS respondentes
            FROM {table('gold_mercado_profissionais')}
            GROUP BY ano_pesquisa, cargo_padronizado
            ORDER BY ano_pesquisa, respondentes DESC
        """,
        "remuneracao_cargo": f"""
            SELECT ano_pesquisa, dimensao_valor AS cargo_padronizado,
                   respondentes_ponto_medio, salario_medio_aproximado
            FROM {table('gold_remuneracao')}
            WHERE dimensao_tipo = 'cargo' AND ano_pesquisa = 2024
            ORDER BY salario_medio_aproximado DESC
        """,
        "remuneracao_senioridade": f"""
            SELECT ano_pesquisa, dimensao_valor AS senioridade,
                   respondentes_faixa_valida, respondentes_ponto_medio,
                   salario_medio_aproximado, faixa_mediana_ordem
            FROM {table('gold_remuneracao')}
            WHERE dimensao_tipo = 'senioridade'
            ORDER BY ano_pesquisa, senioridade
        """,
        "tecnologias": f"""
            SELECT ano_pesquisa, tecnologia, respondentes_elegiveis,
                   adotantes, percentual_adocao
            FROM {table('gold_tecnologias')}
            WHERE segmento_tipo = 'geral' AND segmento_valor = 'Todos'
            ORDER BY ano_pesquisa, tecnologia
        """,
        "ia_geral": f"""
            SELECT ano_pesquisa, respondentes_validos, adotantes_ia,
                   percentual_adocao_ia, uso_gratuito, uso_pago_proprio,
                   uso_pago_empresa, uso_copilot
            FROM {table('gold_inteligencia_artificial')}
            WHERE segmento_tipo = 'geral' AND segmento_valor = 'Todos'
            ORDER BY ano_pesquisa
        """,
        "ia_senioridade": f"""
            SELECT ano_pesquisa, segmento_valor AS senioridade,
                   respondentes_validos, percentual_adocao_ia
            FROM {table('gold_inteligencia_artificial')}
            WHERE segmento_tipo = 'senioridade'
            ORDER BY ano_pesquisa, senioridade
        """,
        "trabalho": f"""
            SELECT ano_pesquisa, modelo_trabalho_padronizado,
                   SUM(respondentes) AS respondentes
            FROM {table('gold_regiao_trabalho')}
            WHERE modelo_trabalho_padronizado IS NOT NULL
            GROUP BY ano_pesquisa, modelo_trabalho_padronizado
            ORDER BY ano_pesquisa, modelo_trabalho_padronizado
        """,
        "regiao_trabalho": f"""
            SELECT regiao_moradia, modelo_trabalho_padronizado,
                   SUM(respondentes_ponto_medio) AS respondentes_ponto_medio,
                   SUM(salario_medio_aproximado * respondentes_ponto_medio)
                     / NULLIF(SUM(respondentes_ponto_medio), 0) AS salario_medio_aproximado
            FROM {table('gold_regiao_trabalho')}
            WHERE ano_pesquisa = 2024
              AND regiao_moradia IS NOT NULL
              AND modelo_trabalho_padronizado IS NOT NULL
            GROUP BY regiao_moradia, modelo_trabalho_padronizado
            ORDER BY regiao_moradia, modelo_trabalho_padronizado
        """,
    }
    return {name: runner.query(name, sql) for name, sql in queries.items()}


def plot_scorecard(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["kpis"], ["ano_pesquisa", "total_respondentes", "percentual_remoto", "percentual_mulheres", "percentual_ia"])
    latest = frame.sort_values("ano_pesquisa").iloc[-1]
    fig, ax = plt.subplots(figsize=(12, 6.75))
    ax.axis("off")
    fig.suptitle(f"Scorecard executivo — State of Data {int(latest['ano_pesquisa'])}", fontsize=22, fontweight="bold", y=0.94)
    metrics = [
        ("Respondentes", f"{int(latest['total_respondentes']):,}".replace(",", "."), NAVY),
        ("Trabalho remoto", f"{br_number(latest['percentual_remoto'])}%", BLUE),
        ("Mulheres na amostra", f"{br_number(latest['percentual_mulheres'])}%", PURPLE),
        ("Uso profissional de IA", f"{br_number(latest['percentual_ia'])}%", GREEN),
    ]
    for index, (label, value, color) in enumerate(metrics):
        x = 0.04 + index * 0.24
        ax.add_patch(plt.Rectangle((x, 0.35), 0.21, 0.28, transform=ax.transAxes, facecolor="#F8FAFC", edgecolor=LIGHT_GRAY))
        ax.text(x + 0.02, 0.56, label, transform=ax.transAxes, fontsize=11, color=GRAY, va="top")
        ax.text(x + 0.02, 0.43, value, transform=ax.transAxes, fontsize=26, fontweight="bold", color=color, va="top")
    ax.text(0.04, 0.24, "Indicadores calculados com denominadores elegíveis e regras de qualidade auditáveis.", transform=ax.transAxes, color=GRAY)
    return finish_figure(fig, output_dir, "01_scorecard_executivo.png", source)


def plot_diversidade(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["diversidade"], ["ano_pesquisa", "respondentes"])
    pivot = frame.pivot_table(index="ano_pesquisa", columns="genero", values="respondentes", aggfunc="sum", fill_value=0)
    pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
    preferred = ["Feminino", "Masculino", "Outro", "Prefiro não informar"]
    columns = [column for column in preferred if column in pct.columns] + [column for column in pct.columns if column not in preferred]
    pct = pct[columns]
    fig, ax = plt.subplots()
    bottom = pd.Series(0.0, index=pct.index)
    for index, column in enumerate(pct.columns):
        values = pct[column]
        bars = ax.bar(pct.index.astype(str), values, bottom=bottom, label=column, color=PALETTE[index % len(PALETTE)], width=0.62)
        for bar, value, base in zip(bars, values, bottom):
            if value >= 4:
                ax.text(bar.get_x() + bar.get_width() / 2, base + value / 2, f"{br_number(value)}%", ha="center", va="center", color="white", fontweight="bold", fontsize=9)
        bottom += values
    ax.set_title("Diversidade de gênero permaneceu praticamente estável")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Participação na amostra (%)")
    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_ylim(0, 100)
    ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.13))
    return finish_figure(fig, output_dir, "02_diversidade_genero.png", source)


def plot_mercado(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["mercado"], ["ano_pesquisa", "respondentes"])
    year = int(frame["ano_pesquisa"].max())
    top = frame[frame["ano_pesquisa"] == year].groupby("cargo_padronizado", as_index=False)["respondentes"].sum().nlargest(8, "respondentes").sort_values("respondentes")
    total = frame[frame["ano_pesquisa"] == year]["respondentes"].sum()
    fig, ax = plt.subplots()
    bars = ax.barh(top["cargo_padronizado"], top["respondentes"], color=BLUE)
    for bar, value in zip(bars, top["respondentes"]):
        ax.text(value + total * 0.006, bar.get_y() + bar.get_height() / 2, f"{int(value):,}  |  {br_number(value / total * 100)}%".replace(",", ".", 1), va="center", fontsize=9)
    ax.set_title(f"Estrutura do mercado de dados — principais cargos em {year}")
    ax.set_xlabel("Respondentes")
    ax.set_ylabel("")
    ax.set_xlim(0, max(top["respondentes"]) * 1.25)
    return finish_figure(fig, output_dir, "03_estrutura_mercado.png", source)


def plot_remuneracao_cargo(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["remuneracao_cargo"], ["ano_pesquisa", "respondentes_ponto_medio", "salario_medio_aproximado"])
    frame = frame[frame["respondentes_ponto_medio"] >= 50].nlargest(8, "salario_medio_aproximado").sort_values("salario_medio_aproximado")
    fig, ax = plt.subplots()
    bars = ax.barh(frame["cargo_padronizado"], frame["salario_medio_aproximado"], color=NAVY)
    for bar, value, count in zip(bars, frame["salario_medio_aproximado"], frame["respondentes_ponto_medio"]):
        ax.text(value + 180, bar.get_y() + bar.get_height() / 2, f"{money(value)}  (n={int(count)})", va="center", fontsize=9)
    ax.set_title("Perfis mais valorizados — remuneração média aproximada em 2024")
    ax.set_xlabel("Salário mensal aproximado")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: money(value)))
    ax.set_xlim(0, max(frame["salario_medio_aproximado"]) * 1.35)
    return finish_figure(fig, output_dir, "04_remuneracao_por_cargo.png", source)


def plot_progressao_salarial(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["remuneracao_senioridade"], ["ano_pesquisa", "salario_medio_aproximado"])
    fig, ax = plt.subplots()
    order = ["Júnior", "Pleno", "Sênior"]
    for index, seniority in enumerate(order):
        group = frame[frame["senioridade"] == seniority].sort_values("ano_pesquisa")
        if group.empty:
            continue
        ax.plot(group["ano_pesquisa"], group["salario_medio_aproximado"], marker="o", linewidth=2.8, markersize=7, label=seniority, color=PALETTE[index])
        for _, row in group.iterrows():
            ax.annotate(money(row["salario_medio_aproximado"]), (row["ano_pesquisa"], row["salario_medio_aproximado"]), xytext=(0, 9), textcoords="offset points", ha="center", fontsize=8)
    ax.set_title("Progressão salarial por senioridade")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Salário mensal aproximado")
    ax.set_xticks(sorted(frame["ano_pesquisa"].dropna().unique()))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: money(value)))
    ax.set_ylim(3000, 16000)
    ax.legend(title="Senioridade", loc="upper center", bbox_to_anchor=(0.5, 1.01), ncol=3)
    return finish_figure(fig, output_dir, "05_progressao_salarial.png", source)


def plot_tecnologias(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["tecnologias"], ["ano_pesquisa", "percentual_adocao"])
    selected = ["SQL", "Python", "Databricks"]
    fig, ax = plt.subplots()
    for index, technology in enumerate(selected):
        group = frame[frame["tecnologia"] == technology].sort_values("ano_pesquisa")
        if group.empty:
            continue
        ax.plot(group["ano_pesquisa"], group["percentual_adocao"], marker="o", linewidth=2.8, markersize=7, label=technology, color=PALETTE[index])
        for _, row in group.iterrows():
            ax.annotate(f"{br_number(row['percentual_adocao'])}%", (row["ano_pesquisa"], row["percentual_adocao"]), xytext=(0, 9), textcoords="offset points", ha="center", fontsize=8)
    ax.set_title("SQL, Python e Databricks ampliaram presença")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Adoção entre respondentes elegíveis (%)")
    ax.set_xticks(sorted(frame["ano_pesquisa"].dropna().unique()))
    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_ylim(0, 100)
    ax.legend()
    ax.text(0.01, -0.16, "Cloud não foi incluída: em 2023 a pergunta mede preferência, não uso.", transform=ax.transAxes, color=GRAY, fontsize=9)
    return finish_figure(fig, output_dir, "06_adocao_tecnologias.png", source)


def plot_ia_geral(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["ia_geral"], ["ano_pesquisa", "percentual_adocao_ia", "respondentes_validos"])
    fig, ax = plt.subplots()
    bars = ax.bar(frame["ano_pesquisa"].astype(int).astype(str), frame["percentual_adocao_ia"], color=[CYAN, BLUE][: len(frame)], width=0.58)
    for bar, value, count in zip(bars, frame["percentual_adocao_ia"], frame["respondentes_validos"]):
        ax.text(bar.get_x() + bar.get_width() / 2, value + 2, f"{br_number(value)}%\nn={int(count):,}".replace(",", "."), ha="center", fontweight="bold")
    ax.set_title("IA generativa já é prática dominante")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Uso profissional entre respostas válidas (%)")
    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_ylim(0, 108)
    return finish_figure(fig, output_dir, "07_adocao_ia_generativa.png", source)


def plot_modelos_trabalho(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path:
    frame = numeric(data["trabalho"], ["ano_pesquisa", "respondentes"])
    pivot = frame.pivot_table(index="ano_pesquisa", columns="modelo_trabalho_padronizado", values="respondentes", aggfunc="sum", fill_value=0)
    pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
    preferred = ["Remoto", "Híbrido flexível", "Híbrido fixo", "Presencial"]
    columns = [column for column in preferred if column in pct.columns]
    pct = pct[columns]
    fig, ax = plt.subplots()
    bottom = pd.Series(0.0, index=pct.index)
    for index, column in enumerate(pct.columns):
        values = pct[column]
        bars = ax.bar(pct.index.astype(str), values, bottom=bottom, label=column, color=PALETTE[index], width=0.62)
        for bar, value, base in zip(bars, values, bottom):
            if value >= 7:
                ax.text(bar.get_x() + bar.get_width() / 2, base + value / 2, f"{br_number(value)}%", ha="center", va="center", color="white", fontsize=8, fontweight="bold")
        bottom += values
    ax.set_title("Flexibilidade permaneceu como padrão de trabalho")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Participação entre respostas válidas (%)")
    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_ylim(0, 100)
    ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.13))
    return finish_figure(fig, output_dir, "08_modelos_trabalho.png", source)


def plot_heatmap_regiao(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path | None:
    if "regiao_trabalho" not in data:
        return None
    frame = numeric(data["regiao_trabalho"], ["salario_medio_aproximado"])
    heat = frame.pivot_table(index="regiao_moradia", columns="modelo_trabalho_padronizado", values="salario_medio_aproximado", aggfunc="mean")
    if heat.empty:
        return None
    fig, ax = plt.subplots()
    image = ax.imshow(heat.values, cmap="Blues", aspect="auto")
    ax.set_xticks(range(len(heat.columns)), heat.columns, rotation=20, ha="right")
    ax.set_yticks(range(len(heat.index)), heat.index)
    for row in range(len(heat.index)):
        for column in range(len(heat.columns)):
            value = heat.iloc[row, column]
            if pd.notna(value):
                ax.text(column, row, money(value), ha="center", va="center", color="white" if value > heat.stack().median() else NAVY, fontsize=8, fontweight="bold")
    fig.colorbar(image, ax=ax, label="Salário mensal aproximado")
    ax.set_title("Remuneração por região e modelo de trabalho — 2024")
    ax.set_xlabel("")
    ax.set_ylabel("")
    return finish_figure(fig, output_dir, "09_heatmap_regiao_trabalho.png", source)


def plot_ia_tipos(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path | None:
    frame = data.get("ia_geral")
    columns = ["uso_gratuito", "uso_pago_proprio", "uso_pago_empresa", "uso_copilot"]
    if frame is None or not set(columns).issubset(frame.columns):
        return None
    frame = numeric(frame, ["ano_pesquisa", *columns]).sort_values("ano_pesquisa")
    labels = {"uso_gratuito": "Gratuito", "uso_pago_proprio": "Pago pelo profissional", "uso_pago_empresa": "Pago pela empresa", "uso_copilot": "Copilot"}
    fig, ax = plt.subplots()
    bottom = pd.Series(0.0, index=frame.index)
    x = frame["ano_pesquisa"].astype(int).astype(str)
    for index, column in enumerate(columns):
        values = frame[column].fillna(0)
        ax.bar(x, values, bottom=bottom, label=labels[column], color=PALETTE[index], width=0.58)
        bottom += values
    ax.set_title("Tipos de acesso à IA generativa — menções")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Quantidade de menções")
    ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.13))
    ax.text(0.01, -0.20, "Pergunta de resposta múltipla: as categorias não somam 100%.", transform=ax.transAxes, color=GRAY, fontsize=9)
    return finish_figure(fig, output_dir, "10_tipos_acesso_ia.png", source)


def plot_ia_senioridade(data: dict[str, pd.DataFrame], output_dir: Path, source: str) -> Path | None:
    if "ia_senioridade" not in data:
        return None
    frame = numeric(data["ia_senioridade"], ["ano_pesquisa", "percentual_adocao_ia"])
    pivot = frame.pivot_table(index="ano_pesquisa", columns="senioridade", values="percentual_adocao_ia", aggfunc="mean")
    order = [column for column in ["Júnior", "Pleno", "Sênior"] if column in pivot.columns]
    pivot = pivot[order]
    fig, ax = plt.subplots()
    pivot.plot(kind="bar", ax=ax, color=PALETTE[: len(order)], width=0.72)
    for container in ax.containers:
        ax.bar_label(container, fmt="%.1f%%", padding=3, fontsize=8)
    ax.set_title("Adoção de IA por senioridade")
    ax.set_xlabel("Ano da pesquisa")
    ax.set_ylabel("Adoção entre respostas válidas (%)")
    ax.set_xticklabels([str(int(value)) for value in pivot.index], rotation=0)
    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_ylim(0, 110)
    ax.legend(title="Senioridade")
    return finish_figure(fig, output_dir, "11_ia_por_senioridade.png", source)


def upload_outputs(paths: list[Path], s3_uri: str, region: str) -> list[str]:
    import boto3

    bucket, prefix = AthenaRunner.split_s3_uri(s3_uri.rstrip("/") + "/placeholder")
    prefix = prefix.rsplit("/", 1)[0]
    client = boto3.client("s3", region_name=region)
    uploaded = []
    for path in paths:
        key = f"{prefix}/{path.name}" if prefix else path.name
        content_type = "image/png" if path.suffix.lower() == ".png" else "application/json"
        client.upload_file(str(path), bucket, key, ExtraArgs={"ContentType": content_type})
        uploaded.append(f"s3://{bucket}/{key}")
    return uploaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["local", "athena"], default=os.getenv("SOURCE", "local"))
    parser.add_argument("--project-dir", type=Path, default=PROJECT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--bucket", default=os.getenv("BUCKET", "lab-255375488094"))
    parser.add_argument("--database", default=os.getenv("DATABASE", "fiap_tech_challenge_fase3"))
    parser.add_argument("--workgroup", default=os.getenv("ATHENA_WORKGROUP", "primary"))
    parser.add_argument("--athena-output", default=None)
    parser.add_argument("--s3-output", default=None)
    # O Glue acrescenta argumentos internos, como --JOB_NAME e --TempDir.
    # Eles não fazem parte da lógica dos gráficos e devem ser ignorados.
    args, _unknown = parser.parse_known_args()
    return args


def main() -> None:
    args = parse_args()
    configure_style()
    runner = None
    if args.source == "athena":
        athena_output = args.athena_output or f"s3://{args.bucket}/data-output/athena-results/"
        runner = AthenaRunner(args.region, args.database, athena_output, args.workgroup)
        data = load_athena(runner)
        source_note = "Fonte: State of Data Brasil 2022–2024 | AWS Glue Data Catalog e Athena"
    else:
        data = load_local(args.project_dir)
        source_note = "Fonte: State of Data Brasil 2022–2024 | Artefatos analíticos validados"

    plots = [
        plot_scorecard(data, args.output_dir, source_note),
        plot_diversidade(data, args.output_dir, source_note),
        plot_mercado(data, args.output_dir, source_note),
        plot_remuneracao_cargo(data, args.output_dir, source_note),
        plot_progressao_salarial(data, args.output_dir, source_note),
        plot_tecnologias(data, args.output_dir, source_note),
        plot_ia_geral(data, args.output_dir, source_note),
        plot_modelos_trabalho(data, args.output_dir, source_note),
        plot_heatmap_regiao(data, args.output_dir, source_note),
        plot_ia_tipos(data, args.output_dir, source_note),
        plot_ia_senioridade(data, args.output_dir, source_note),
    ]
    paths = [path for path in plots if path is not None]
    manifest: dict[str, Any] = {
        "status": "ok",
        "source": args.source,
        "charts": [path.name for path in paths],
        "athena_queries": runner.executions if runner else [],
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    upload_target = args.s3_output
    if args.source == "athena" and upload_target is None:
        upload_target = f"s3://{args.bucket}/data-output/graficos-executivos/"
    if upload_target:
        manifest["s3_objects"] = upload_outputs(paths, upload_target, args.region)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        upload_outputs([manifest_path], upload_target, args.region)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
