import os
from .bronze.stg_vehicle import loading_data_pipeline
from dagster import ConfigurableIOManager, Definitions, asset
from pyspark.sql import SparkSession
from .bronze.stg_vehicle import loading_data_pipeline


class LocalParquetIOManager(ConfigurableIOManager):
    def _get_path(self, context):
        return os.path.join(*context.asset_key.path)

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))


@asset
def bronze_asset():
    result = loading_data_pipeline.execute_in_process()
    df = result.output_for_node("reading_data_frame")
    if df is None:
        raise ValueError("Pipeline returned None instead of a DataFrame")
    return df


defs = Definitions(
    assets=[bronze_asset],
    jobs=[loading_data_pipeline],
    resources={"io_manager": LocalParquetIOManager()},
)
