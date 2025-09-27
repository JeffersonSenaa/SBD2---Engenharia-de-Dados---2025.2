from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL_Lakehouse"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    return spark


def get_postgres_props():
    return {
        "url": "jdbc:postgresql://postgres:5432/sbd1",
        "properties": {
            "user": "sbd1",
            "password": "sbd1",
            "driver": "org.postgresql.Driver"
        }
    }
