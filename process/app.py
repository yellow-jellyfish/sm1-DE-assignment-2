from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType


builder = (
    SparkSession.builder.enableHiveSupport()
    .appName("assignment-2")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
)

spark = builder.getOrCreate()

def main(path="gs://covid19_cases3/"):
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("delimiter", ",")
        .load(path)
    )

    # Fill empty cells with null
    df = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])

    # Make new column with partial vaccinated people minus fully vaccinated people
    df = df.withColumn("people_incomplete_vaccination", df.select(df.people_vaccinated - df.people_fully_vaccinated))

    # Drop few columns with less than 1.5% non-null values
    df.drop("weekly_icu_admissions", "weekly_icu_admissions_per_million", "weekly_hosp_admissions", "weekly_hosp_admissions_per_million")

    # partition by date

    df.show()

    df.write.mode("overwrite").format(
        "bigquery"
    ).option("temporaryGcsBucket", "temp-gcs-bucket3").save(
        "jadsdataengineering.covid19_cases2"
    )


if __name__ == "__main__":
    main()
