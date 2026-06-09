from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")

# def get_spark() -> SparkSession:
#     return SparkSession.getActiveSession()

# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

def main():
    get_taxis(get_spark()).show(5)


if __name__ == "__main__":
    main()
