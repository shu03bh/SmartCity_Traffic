import boto3
import json
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Helper: Fetch secret from AWS Secrets Manager

def get_secret(secret_name, region_name="us-east-2"):
    """Retrieve secret JSON from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as e:
        print(f"[ERROR] Unable to retrieve secret: {e}")
        raise


# Spark Session Setup

def create_spark_session():
    spark = SparkSession.builder \
        .appName("glueCatalog_to_RDS") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-warehouse-path/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    print("[INFO] Spark session created successfully")
    return spark, glueContext


# Load Data from Glue Catalog

def load_data(spark, catalog_db, catalog_table):
    # Directly read Iceberg table via Spark
    df = spark.table(f"glue_catalog.{catalog_db}.{catalog_table}")
    print(f"[INFO] Data loaded from Glue Catalog: {catalog_db}.{catalog_table}")
    return df


# Write Data to RDS

def write_to_rds(df, jdbc_url, connection_properties, target_table):
    df.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode="overwrite",  # Overwrite existing table
        properties=connection_properties
    )
    print(f"[INFO] Data written to RDS table: {target_table}")


# Main Orchestration

def main():
    # Fetch secrets for Glue Catalog and RDS connection
    secret_name = "smartcity/glue_to_rds"   # <-- name of your secret in Secrets Manager
    secrets = get_secret(secret_name, region_name="us-east-2")

    catalog_db = secrets.get("catalog_db", "smartcity_traffic_db")
    catalog_table = secrets.get("catalog_table", "traffic_alerts")

    jdbc_url = secrets.get("jdbc_url", "jdbc:postgresql://smartcity-traffic.c1ykaosoelhu.us-east-2.rds.amazonaws.com:5432/smart_city_traffic_kpi")
    connection_properties = {
        "user": secrets.get("user", "capstoneteam02"),
        "password": secrets.get("password", "capstoneteam02"),
        "driver": "org.postgresql.Driver"
    }
    target_table = secrets.get("target_table", "traffic_kpi")

    # Spark + Glue setup
    spark, glueContext = create_spark_session()

    # Load data from Glue Catalog
    df = load_data(spark, catalog_db, catalog_table)

    # Write to RDS
    write_to_rds(df, jdbc_url, connection_properties, target_table)

if __name__ == "__main__":
    main()
