from pyspark.sql import SparkSession 
spark = SparkSession.builder \
  .appName('1.2. BigQuery Storage & Spark SQL - Python')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar') \
  .getOrCreate()


def main (input_uri, output_table, output_dataset):
    #spark=SparkSession.builder.appName("CSV to BigQuery").config('spark.jars', 'gs://beam_demo_at/spark-2.4-bigquery-0.30.0.jar').getOrCreate()
    spark = SparkSession.builder.appName("CSV to BigQuery").getOrCreate()
    
    df=spark.read.csv(input_uri, header=True, inferSchema=True)
    print(df.show())
    #df.write.format("bigquery").option("temporaryGcsBucket", "gs://beam_demo_at").option("table",output_table).option("dataset", output_dataset).mode("overwrite").save()
    df.write.format("bigquery").option("temporaryGcsBucket", "beam_demo_at").option("table", 'gcp-accelerator-380712.output_dataset.output_table').mode("append").save()
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="pySpark job for CSV to BQ import")
    parser.add_argument("--input_uri",required=True, help="GCS URI of input CSV file")
    parser.add_argument("--output_table",required=True, help="BQ table Name")
    parser.add_argument("--output_dataset",required=True, help="BQ dataset Name")
    
    args = parser.parse_args()
    
    main(args.input_uri,args.output_table, args.output_dataset)
    
