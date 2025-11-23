import boto3
import json
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, avg, sum, window, when
from pyspark.sql.utils import AnalysisException

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("glue_traffic_etl") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.type", "glue") \
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3://smartcity-processed/warehouse") \
            .getOrCreate()
        glueContext = GlueContext(spark.sparkContext)
        print("[INFO] Spark session created successfully")
        return spark, glueContext
    except Exception as e:
        print(f"[ERROR] Failed to create Spark session: {e}")
        raise

def load_raw_data(glueContext, catalog_db, catalog_table):
    try:
        dyn_frm = glueContext.create_dynamic_frame.from_catalog(
            database=catalog_db,
            table_name=catalog_table
        )
        df = dyn_frm.toDF()
        print(f"[INFO] Raw data loaded successfully from Glue Catalog: {catalog_db}.{catalog_table}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read raw data: {e}")
        raise

def cast_timestamp(df):
    return df.withColumn("timestamp", col("timestamp").cast("timestamp"))

def aggregate_five_minute(df):
    return df.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("zone")
    ).agg(
        avg("avg_speed").alias("avg_speed"),
        sum("vehicle_count").alias("vehicle_count_total"),
        avg("congestion_index").alias("rolling_congestion_index")
    ).withColumn(
        "critical_flag", when(col("rolling_congestion_index") > 0.70, "CRITICAL").otherwise("NORMAL")
    )

def aggregate_hourly(df):
    return df.groupBy(
        window(col("timestamp"), "1 hour"),
        col("zone")
    ).agg(avg("congestion_index").alias("hourly_congestion"))

def write_to_iceberg(df):
    df_flat = df.withColumn("window_start", col("window.start")) \
                .withColumn("window_end", col("window.end")) \
                .drop("window")
    df_flat.writeTo('glue_catalog.smartcity_traffic_db.traffic_alerts').createOrReplace()
    print("[INFO] Aggregated data written to Iceberg table successfully")
    return df_flat

def export_critical_alerts(df):
    critical_df = df.filter(col("critical_flag") == "CRITICAL")
    critical_df.write.format("csv").option("header", "true").mode("overwrite") \
        .save("s3://smartcity-processed/critical_alerts/")
    print("[INFO] Critical alerts exported to S3 successfully")

def main():
    # Get secrets
    secret_name = "smartcity/glue_catalog"
    secrets = get_secret(secret_name, region_name="us-east-2")
    catalog_db = secrets.get("catalog_db", "smartcity_traffic_db")
    catalog_table = secrets.get("catalog_table", "streaming_firehose_row")

    # Spark + Glue
    spark, glueContext = create_spark_session()

    # Load + transform
    raw_df = load_raw_data(glueContext, catalog_db, catalog_table)
    raw_df = cast_timestamp(raw_df)

    # Aggregations
    agg_df = aggregate_five_minute(raw_df)
    hourly_df = aggregate_hourly(raw_df)

    # Write outputs
    agg_df_flat = write_to_iceberg(agg_df)
    export_critical_alerts(agg_df_flat)

if __name__ == "__main__":
    main()
