"""Persist small result tables as CSV + Markdown preview for REPORT.md."""

import math
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame

from imdb_project import config


def _fmt(value) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:,.2f}" if abs(value) >= 1000 else f"{value:.2f}"
    if isinstance(value, (list, tuple)):
        return ", ".join(_fmt(v) for v in value)
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= 120 else text[:117] + "..."


def to_markdown(rows, columns) -> str:
    """Render rows (sequence of sequences) as a GitHub-flavoured table."""
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(v) for v in row) + " |")
    return "\n".join(lines) + "\n"


def save(df: DataFrame, section: str, name: str, preview: int = 25,
         title: str = "") -> int:
    """Write `df` to results/<section>/<name>.csv and a Markdown preview.

    The DataFrame is expected to be already sorted. Returns the row count.
    """
    out_dir = config.RESULTS_DIR / section
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    # Integer columns containing nulls come back as float: restore them.
    for col in pdf.columns:
        if pdf[col].dtype.kind == "f":
            values = pdf[col].dropna()
            if len(values) and (values % 1 == 0).all() \
                    and not col.lower().endswith(("rating", "avg", "mean")):
                pdf[col] = pdf[col].astype("Int64")
    pdf.to_csv(out_dir / f"{name}.csv", index=False)
    columns = list(pdf.columns)
    rows = pdf.head(preview).itertuples(index=False, name=None)
    header = f"**{title}** — " if title else ""
    md = (f"{header}{len(pdf):,} rows (showing {min(preview, len(pdf))}; "
          f"full table: `results/{section}/{name}.csv`)\n\n")
    md += to_markdown(rows, columns)
    (out_dir / f"{name}.md").write_text(md, encoding="utf-8")
    print(f"[results] {section}/{name}: {len(pdf):,} rows")
    print(md[:3000])
    return len(pdf)


def save_text(section: str, name: str, text: str) -> Path:
    out_dir = config.RESULTS_DIR / section
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(text, encoding="utf-8")
    return path
