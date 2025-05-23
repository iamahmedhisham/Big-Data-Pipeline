from pyspark.sql import SparkSession

def main():
    # 1. Create a SparkSession
    spark = SparkSession.builder \
        .appName("ReadParquetExample") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # 2. Read the Parquet files from an HDFS folder
    #    Adjust the path if your output folder is different
    parquet_path = "hdfs://namenode:9000/output_parquet"
    df = spark.read.parquet(parquet_path)

    # 3. Print the first 10 rows
    print("=== Showing top 10 rows from the Parquet dataset ===")
    df.show(10)

    # 4. Print record count
    record_count = df.count()
    print("Total number of rows: {}".format(record_count))

    # 5. Print schema
    df.printSchema()

    # 6. Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
