# **AWS Smart City Traffic Analytics — Capstone Project**

## **Overview**
A real-time data engineering project to build a **Smart City Traffic Analytics Platform** on AWS. The system ingests continuous sensor data (vehicle counts, speeds, congestion levels), transforms and aggregates it using **AWS Glue with Apache Iceberg**, and delivers near real-time dashboards in **Amazon QuickSight**.

---

## **Table of Contents**
- [Project Objectives](#project-objectives)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [AWS Services Used](#aws-services-used)
- [Sample Datasets](#sample-datasets)
- [Setup Steps](#setup-steps)
- [ETL Aggregations](#etl-aggregations)
- [Dashboard Metrics](#dashboard-metrics)
- [Security & Best Practices](#security--best-practices)
- [Extensions](#extensions)
- [Dashboards](#dashboards)
- [Final Outcome](#final-outcome)

---

## **Project Objectives**
- Design a streaming data pipeline using **Kinesis Data Streams** and **Firehose**.
- Implement ETL and aggregation with **AWS Glue (PySpark)** and store data in **Iceberg format**.
- Query historical and live traffic data using **Athena** and **RDS**.
- Build visual dashboards for congestion analysis with **QuickSight**.
- Apply data governance using **Lake Formation**.
- Monitor and optimize real-time data pipelines in AWS.

---

## **Architecture**
```
Traffic Sensors / Simulators
↓
Amazon Kinesis Data Stream
↓
Amazon Kinesis Firehose
↓
Amazon S3 (Raw Zone)
↓
AWS Glue ETL (PySpark) → Apache Iceberg Tables → S3 (Processed Zone)
↓
Amazon Athena & Amazon RDS (Reporting Layer)
↓
Amazon QuickSight Dashboards (Heatmaps, KPIs)
```
Additional:
- **Lake Formation** for governance
- **CloudWatch / CloudTrail** for monitoring
- **IAM** for roles and permissions

---

## **Prerequisites**
- AWS Account with admin access
- IAM roles for Glue, Firehose, Athena, QuickSight
- Python 3.x for local scripts
- AWS CLI configured
- QuickSight subscription enabled

---

## **AWS Services Used**
- **Amazon Kinesis Data Streams / Firehose** — Stream ingestion
- **Amazon S3** — Raw and curated storage zones
- **AWS Glue (PySpark)** — Transformation and aggregation
- **Apache Iceberg** — Lakehouse format for ACID datasets
- **Amazon Athena** — Query Iceberg tables
- **Amazon RDS** — Store aggregated KPI tables
- **Amazon QuickSight** — Dashboards
- **AWS Lake Formation** — Access control
- **IAM, CloudWatch, CloudTrail** — Security and observability

---

## **Sample Datasets**
- **traffic_stream.json** — Simulated live events
Fields: `camera_id`, `timestamp`, `zone`, `vehicle_count`, `avg_speed`, `congestion_index`, `status`
- **camera_metadata.csv** — Static metadata of traffic cameras
Fields: `camera_id`, `zone`, `latitude`, `longitude`, `installation_year`

---

## **Setup Steps**
1. **Create S3 buckets**:
- `streaming_firehose_row`
- `smartcity-traffic-processed`
2. **Create Kinesis Data Stream**: `new-stream`
3. **Create Firehose delivery stream** → S3 raw zone
4. **Set IAM roles** for Glue, Firehose, Athena
5. **Run traffic generator**:
```bash
python gen_traffic_stream.py
```
6. **Glue Crawler**: Crawl raw S3 path and register schema
7. **Run Glue ETL job**:
```bash
glue_traffic_etl.py
```
8. **Athena Queries**:
Execute `athena/traffic_ddl.sql` for congestion analysis
9. **QuickSight Dashboards**: Connect to Athena or RDS
10. **Apply Lake Formation policies** for access control
11. **Monitor**: Use CloudWatch for Firehose and Glue metrics
12. **Cleanup**: Run teardown script

---

## **ETL Aggregations**
- Average speed
- 5-minute rolling congestion index
- Vehicle count totals
- Critical zone flags
- Per-hour congestion metrics

---

## **Dashboard Metrics**
- Average speed
- 5-minute rolling congestion index
- Vehicle count totals
- Critical zone flags
- Per-hour congestion trends

---

## **Security & Best Practices**
- Encrypt S3 buckets with SSE-KMS
- Use IAM roles with least privilege
- Enable CloudTrail and CloudWatch
- Apply Lake Formation for fine-grained access
- Optimize Firehose buffering and Glue job intervals

---

## **Extensions**
- **Predictive Modeling with SageMaker**:
Train a regression or time-series model to forecast congestion index.
- **IoT Core Integration**: Direct sensor-to-stream connectivity.
- **Infrastructure as Code**: Automate setup with Terraform or AWS CDK.

---

## **Dashboards**
*(Add screenshots or links to QuickSight dashboards here)*
- Congestion Heatmap
- Zone-wise KPIs
- Rolling averages visualization

---

## **Final Outcome**
A fully functional **Smart City Traffic Analytics Platform** on AWS that:
- Ingests real-time traffic data
- Processes and aggregates using Glue + Iceberg
- Provides governed access via Lake Formation
- Delivers actionable insights through QuickSight dashboards
- Supports predictive modeling for future congestion trends
