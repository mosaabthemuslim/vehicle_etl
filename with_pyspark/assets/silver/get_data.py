import dagster as dg
from ...definitions import bronze_asset


@dg.asset(deps=[bronze_asset], required_resource_keys={"pyspark"})
def get_dataframe(context):
    spark = context.resources.pyspark.spark_session

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/vehicledb")
            .option("dbtable", "bronze.staging")
            .option("user", "postgres")
            .option("password", "postgres")
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        context.log.info("Successfully read from PostgreSQL")
        df.show(5)
        return df
    except Exception as e:
        context.log.error(f"Error reading from PostgreSQL: {str(e)}")
        raise
