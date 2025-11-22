from pyspark.sql import SparkSession
from awsglue.context import GlueContext

spark = SparkSession.builder \
        .appName("glueCatalog_to_RDS") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-warehouse-path/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
        
catalog_db='smartcity_traffic_db'
catalog_table='traffic_alerts'

df=spark.table("glue_catalog.smartcity_traffic_db.traffic_alerts")

# dyn_frm=glueContext.create_dynamic_frame.from_catalog(database=catalog_db,table_name=catalog_table)

# df=dyn_frm.toDF()

jdbc_url = "jdbc:postgresql://smartcity-traffic.c1ykaosoelhu.us-east-2.rds.amazonaws.com:5432/smart_city_traffic_kpi"
connection_properties = {
    "user": "capstoneteam02",
    "password": "capstoneteam02",
    "driver": "org.postgresql.Driver"
}    

df.write.jdbc(
    url=jdbc_url,
    table="traffic_kpi",  # Replace with your target table name
    mode="overwrite", # Example: Overwrite the table if it exists
    properties=connection_properties
)
