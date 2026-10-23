"""
spark_utils.py

Shared helper module for the Olist PySpark + Delta Lake project. Import get_spark_session() into every notebook instead of re-writing
the SparkSession + Delta boilerplate each time.

Usage in a notebook cell:
    from spark_utils import get_spark_session
    spark = get_spark_session("BronzeIngestion")
"""

from pyspark.sql import SparkSession               # core Spark session class
from delta import configure_spark_with_delta_pip    # Delta's helper that wires up the correct JARs for the installed delta-spark version


def get_spark_session(app_name: str) -> SparkSession:
    """
    Build (or reuse) a SparkSession with Delta Lake's SQL extensions enabled.

    Parameters
    ----------
    app_name : str
        Name shown in the Spark UI for this session — pass something specific per notebook, e.g. "BronzeIngestion", "SilverCleaning",
        "GoldKPIs", so you can tell jobs apart if you ever check the UI.

    Returns
    -------
    SparkSession
        A ready-to-use Spark session with Delta support configured.
    """

    # Start building a SparkSession, tagging it with the given app name
    builder = (
        SparkSession.builder
        .appName(app_name)  # label for this session, visible in the Spark UI
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"  # registers Delta's SQL syntax (MERGE, time travel, etc.)
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"  # makes Delta the default table catalog/format
        )
    )

    # configure_spark_with_delta_pip() attaches the correct Delta JARs
    # for whatever delta-spark version is pip-installed in this env
    # (delta-spark==4.4.0 in this project), then getOrCreate() either
    # returns the existing active session or starts a new one.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Cleanly shut down a SparkSession.
    Call this at the end of a notebook once you're done running Spark jobs, so resources (executors, memory) are released properly.
    """
    spark.stop()  # releases the Spark session's resources