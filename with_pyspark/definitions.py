import os

from dagster_pyspark import pyspark_resource
from .assets.bronze.stg_vehicle import loading_data_pipeline
from dagster import ConfigurableIOManager, Definitions, asset
from pyspark.sql import SparkSession
from .assets.bronze.stg_vehicle import bronze_asset
from .assets.silver.get_data import get_dataframe


class LocalParquetIOManager(ConfigurableIOManager):
    def _get_path(self, context):
        return os.path.join(*context.asset_key.path)

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))


defs = Definitions(
    assets=[bronze_asset, get_dataframe],
    jobs=[loading_data_pipeline],
    resources={
        "io_manager": LocalParquetIOManager(),
        "pyspark": pyspark_resource.configured(
            {
                "spark_conf": {
                    "spark.jars": "/home/mosaab/postgresql-42.jar",
                    # You can add other Spark configurations here
                    "spark.app.name": "vehicle_etl",
                }
            }
        ),
    },
)
