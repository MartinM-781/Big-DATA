"""SparkSession factory and small I/O helpers."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from imdb_project import config


def get_spark(app_name: str, **conf: str) -> SparkSession:
    """Return a SparkSession; defaults come from spark-defaults.conf."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_clean(spark: SparkSession, table: str) -> DataFrame:
    """Read one cleaned IMDb table (parquet) by its IMDb name."""
    return spark.read.parquet(str(config.CLEAN_DIR / table.replace(".", "_")))


def write_parquet(df: DataFrame, path: Path, partitions: int = 0) -> None:
    """Overwrite a parquet dataset, optionally repartitioning first."""
    if partitions:
        df = df.repartition(partitions)
    df.write.mode("overwrite").parquet(str(path))
