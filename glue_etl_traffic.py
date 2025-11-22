

from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, avg, sum, window, when
# ---------------------------
# Spark Session with Iceberg Config
# ---------------------------
spark = SparkSession.builder \
    .appName("glue_traffic_etl") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.type", "glue") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://smartcity-processed/warehouse") \
    .getOrCreate()
    
    
glueContext=GlueContext(spark.sparkContext)
# ---------------------------
# Read Raw Data from Glue Catalog Table
# ---------------------------
catalog_db='smartcity_traffic_db'
catalog_table='streaming_firehose_row'

dyn_frm=glueContext.create_dynamic_frame.from_catalog(database=catalog_db,table_name=catalog_table)

raw_df=dyn_frm.toDF()

#raw_df = spark.read.table("AwsDataCatalog.smartcity_traffic_db.streaming_firehose_row")
# Ensure timestamp is in proper format
raw_df = raw_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
# ---------------------------
# Aggregation by Zone and Time Window
# ---------------------------
agg_df = raw_df.groupBy(
    window(col("timestamp"), "5 minutes"),  # Rolling 5-min window
    col("zone")
).agg(
    avg("avg_speed").alias("avg_speed"),
    sum("vehicle_count").alias("vehicle_count_total"),
    avg("congestion_index").alias("rolling_congestion_index")
).withColumn(
    "critical_flag", when(col("rolling_congestion_index") > 0.70, "CRITICAL").otherwise("NORMAL")
)
# Add per-hour congestion metric
hourly_df = raw_df.groupBy(
    window(col("timestamp"), "1 hour"),
    col("zone")
).agg(
    avg("congestion_index").alias("hourly_congestion")
)
# ---------------------------
# Write Aggregated Data to Iceberg Table
agg_df_flat = agg_df.withColumn("window_start", col("window.start")).withColumn("window_end", col("window.end")).drop("window")

# ---------------------------
#agg_df_flat.write.format("iceberg").mode("overwrite").option('overwriteSchema','true').saveAsTable("smartcity_traffic_db.traffic_alerts")

agg_df_flat.writeTo('glue_catalog.smartcity_traffic_db.traffic_alerts').createOrReplace()
#Export CRITICAL Alerts as CSV to S3
# ---------------------------
critical_df = agg_df_flat.filter(col("critical_flag") == "CRITICAL")

critical_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3://smartcity-processed/critical_alerts/")
    
    
    
    