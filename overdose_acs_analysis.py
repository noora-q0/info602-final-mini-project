from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, lpad, round as spark_round,
    when, avg, count, min as spark_min, max as spark_max
)

spark = SparkSession.builder.appName("INFO602 Final Project Overdose ACS Analysis").getOrCreate()

base_path = "/user/taskly_system_gmail_com/info602_final_project/"
output_path = "/user/taskly_system_gmail_com/info602_final_project/output/"

vdh = spark.read.option("header", "true").option("inferSchema", "true").csv(
    base_path + "vdh_overdose_ed_visits_geography.csv"
)

acs = spark.read.option("header", "true").option("inferSchema", "true").csv(
    base_path + "acs_b17018_va_counties_full_clean.csv"
)

vdh_clean = (
    vdh
    .filter(col("Overdose ED Visit Year") == 2025)
    .filter(col("Overdose ED Visit Drug Type") == "All Drug")
    .filter(col("Overdose ED Visit Patient Geography Level") == "Locality")
    .filter(col("Combined Locality") == "No")
    .select(
        col("Overdose ED Visit Patient Geography Name").alias("vdh_geography"),
        col("Overdose ED Visit Patient FIPS").cast("string").alias("fips"),
        col("Overdose ED Visit Count").cast("double").alias("overdose_ed_count"),
        col("Overdose ED Visit Rate per 10,000 visits").cast("double").alias("overdose_ed_rate")
    )
)

acs_clean = (
    acs
    .withColumn("fips", concat(col("state").cast("string"), lpad(col("county").cast("string"), 3, "0")))
    .select(
        col("NAME").alias("acs_county_name"),
        col("fips"),
        col("B17018_001E").cast("double").alias("total_families"),
        col("B17018_002E").cast("double").alias("families_below_poverty"),

        col("B17018_004E").cast("double").alias("below_married_less_hs"),
        col("B17018_005E").cast("double").alias("below_married_hs"),
        col("B17018_010E").cast("double").alias("below_male_less_hs"),
        col("B17018_011E").cast("double").alias("below_male_hs"),
        col("B17018_015E").cast("double").alias("below_female_less_hs"),
        col("B17018_016E").cast("double").alias("below_female_hs"),

        col("B17018_021E").cast("double").alias("above_married_less_hs"),
        col("B17018_022E").cast("double").alias("above_married_hs"),
        col("B17018_027E").cast("double").alias("above_male_less_hs"),
        col("B17018_028E").cast("double").alias("above_male_hs"),
        col("B17018_032E").cast("double").alias("above_female_less_hs"),
        col("B17018_033E").cast("double").alias("above_female_hs")
    )
    .withColumn(
        "poverty_rate",
        spark_round((col("families_below_poverty") / col("total_families")) * 100, 2)
    )
    .withColumn(
        "hs_or_less_families",
        col("below_married_less_hs") + col("below_married_hs") +
        col("below_male_less_hs") + col("below_male_hs") +
        col("below_female_less_hs") + col("below_female_hs") +
        col("above_married_less_hs") + col("above_married_hs") +
        col("above_male_less_hs") + col("above_male_hs") +
        col("above_female_less_hs") + col("above_female_hs")
    )
    .withColumn(
        "hs_or_less_rate",
        spark_round((col("hs_or_less_families") / col("total_families")) * 100, 2)
    )
)

joined = (
    vdh_clean
    .join(acs_clean, on="fips", how="inner")
    .withColumn(
        "poverty_bucket",
        when(col("poverty_rate") < 6, "1. Low poverty: <6%")
        .when(col("poverty_rate") < 10, "2. Moderate poverty: 6% to <10%")
        .when(col("poverty_rate") < 14, "3. High poverty: 10% to <14%")
        .otherwise("4. Very high poverty: 14%+")
    )
    .withColumn(
        "education_bucket",
        when(col("hs_or_less_rate") < 25, "1. Lower HS-or-less: <25%")
        .when(col("hs_or_less_rate") < 35, "2. Moderate HS-or-less: 25% to <35%")
        .when(col("hs_or_less_rate") < 45, "3. High HS-or-less: 35% to <45%")
        .otherwise("4. Very high HS-or-less: 45%+")
    )
)

poverty_summary = (
    joined
    .groupBy("poverty_bucket")
    .agg(
        count("*").alias("locality_count"),
        spark_round(avg("overdose_ed_rate"), 2).alias("avg_overdose_ed_rate"),
        spark_round(avg("poverty_rate"), 2).alias("avg_poverty_rate"),
        spark_round(spark_min("overdose_ed_rate"), 2).alias("min_overdose_ed_rate"),
        spark_round(spark_max("overdose_ed_rate"), 2).alias("max_overdose_ed_rate")
    )
    .orderBy("poverty_bucket")
)

education_summary = (
    joined
    .groupBy("education_bucket")
    .agg(
        count("*").alias("locality_count"),
        spark_round(avg("overdose_ed_rate"), 2).alias("avg_overdose_ed_rate"),
        spark_round(avg("hs_or_less_rate"), 2).alias("avg_hs_or_less_rate"),
        spark_round(spark_min("overdose_ed_rate"), 2).alias("min_overdose_ed_rate"),
        spark_round(spark_max("overdose_ed_rate"), 2).alias("max_overdose_ed_rate")
    )
    .orderBy("education_bucket")
)

top_overdose = (
    joined
    .select(
        "fips",
        "vdh_geography",
        "acs_county_name",
        "overdose_ed_count",
        "overdose_ed_rate",
        "poverty_rate",
        "hs_or_less_rate",
        "poverty_bucket",
        "education_bucket"
    )
    .orderBy(col("overdose_ed_rate").desc())
    .limit(15)
)

final_dataset = joined.select(
    "fips",
    "vdh_geography",
    "acs_county_name",
    "overdose_ed_count",
    "overdose_ed_rate",
    "poverty_rate",
    "hs_or_less_rate",
    "poverty_bucket",
    "education_bucket",
    "total_families",
    "families_below_poverty",
    "hs_or_less_families"
)

print("Clean joined analysis row count:", final_dataset.count())

print("\nPoverty bucket summary:")
poverty_summary.show(truncate=False)

print("\nEducation bucket summary:")
education_summary.show(truncate=False)

print("\nTop 15 localities by overdose ED visit rate:")
top_overdose.show(15, truncate=False)

# Clear previous outputs, if any
import subprocess
subprocess.run(["hadoop", "fs", "-rm", "-r", "-f", output_path], check=False)

final_dataset.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "final_joined_dataset")
poverty_summary.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "poverty_summary")
education_summary.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "education_summary")
top_overdose.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "top_overdose_localities")

print("\nSaved outputs to HDFS:")
print(output_path)

spark.stop()
