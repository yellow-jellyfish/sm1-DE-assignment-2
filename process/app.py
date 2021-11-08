from pyspark.sql import SparkSession


builder = (
    SparkSession
    .builder
    .enableHiveSupport()
    .appName("assignment-2"))

spark = builder.getOrCreate()


def main(path="gs://covid19-twitter/"):
    df = spark.read.csv.option("delimiter", "\t").load(path)
    df.show()
    df.printSchema()


if __name__ == "__main__":
    main()