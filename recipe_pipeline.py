from pyspark.sql import SparkSession
import ingest_data, tranform
import logging

logging.basicConfig(filename="recipe_pipeline.log", level=logging.INFO, \
                    format="%(levelname)s:%(message)s")


class Pipeline:

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("HelloFreshProject")\
            .getOrCreate()


    def run_pipelile(self, input_dataset_path, input_schema_path, curated_dataset_path, processed_data_path):

        # logging.config.fileConfig("logs/logging.conf")

        ingest_process = ingest_data.ingest(self.spark)
        transformation = tranform.transform_data(self.spark)

        # Task-1 Ingestion code
        raw_df = ingest_process.read_data("JSON", input_schema_path, input_dataset_path)
        refined_df = ingest_process.refine_data(raw_df)
        ingest_process.ingest_data("parquet", curated_dataset_path, "curated_recipes_data", refined_df)

        # Task-2 Transformation
        final_df = transformation.complexity_transformation(refined_df)
        ingest_process.ingest_data("csv", processed_data_path, "processed_recipes_data", final_df)
        self.spark.stop()


if __name__ == '__main__':

    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info('Spark Session created')
    logging.info('Pipeline started')
    pipeline.run_pipelile("input/", "schemas/input_schema.json", "output/curated", "output/processed")
    logging.info('Pipeline executed')


