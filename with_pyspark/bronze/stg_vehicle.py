from dagster import op, job
from dagster_pyspark import pyspark_resource

custom_pyspark = pyspark_resource.configured(
    {
        "spark_conf": {
            "spark.jars": "/home/mosaab/postgresql-42.jar",  # Fixed typo in 'jar' extension
        }
    }
)


@op(required_resource_keys={"pyspark"}, name="reading_data_frame")
def read_df(context):
    spark = spark = context.resources.pyspark.spark_session
    df = spark.read.csv(
        path="/home/mosaab/Projects/vehicle_etl/with_pyspark/data/data.csv",
        sep=",",
        header=True,
        inferSchema=True,
    )
    return df


@op(required_resource_keys={"pyspark"}, name="write_to_postgres")
def write_to_db(df):
    (
        df.write.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/vehicledb")
        .option("dbtable", "bronze.staging")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


@job(resource_defs={"pyspark": custom_pyspark})
def loading_data_pipeline():

    df = read_df()

    write_to_db(df)
