# YouTube Trending Data Pipeline 🎥📊

![AWS](https://img.shields.io/badge/AWS-FF9900?style=flat-square&logo=amazon-aws)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)


A production-grade, cloud-native ETL pipeline that ingests YouTube trending video data across 10 regions, transforms it through a medallion architecture (Bronze → Silver → Gold), enforces automated data quality gates, and produces analytics-ready aggregations — all orchestrated by AWS Step Functions.

**[Features](#-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Documentation](#-documentation) • 

## 🌟 Features

### Core Capabilities
- ✅ **Multi-Region Data Collection** — Automated ingestion from YouTube API v3 across 10 countries
- ✅ **Medallion Architecture** — Industry-standard Bronze/Silver/Gold layered data processing
- ✅ **Automated Quality Gates** — Built-in validation prevents bad data from reaching analytics
- ✅ **Serverless & Scalable** — Zero infrastructure management with AWS Lambda + Glue
- ✅ **Cost-Optimized Storage** — Parquet with Snappy compression reduces storage by 80%+
- ✅ **Business-Ready Analytics** — Pre-built trending, channel, and category aggregation tables
- ✅ **Production Monitoring** — CloudWatch + SNS alerts for pipeline health tracking

### Technical Highlights
- **Orchestration:** AWS Step Functions with retry logic and parallel execution
- **Data Format:** Parquet (Snappy) for optimal query performance
- **Processing:** PySpark on AWS Glue for distributed data transformations
- **Storage:** S3 with intelligent tiering and lifecycle policies
- **Query Engine:** Amazon Athena for ad-hoc SQL analysis
- **Security:** IAM roles with least-privilege access, encrypted S3 buckets

## 🏗️ Architecture

### High-Level System Design

```
                    ┌─────────────────────┐
                    │  YouTube API v3     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Bronze Layer       │
                    │  (Raw JSON in S3)    │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
        ┌──────────────┐           ┌──────────────┐
        │ Glue (PySpark)│           │  Lambda      │
        │ Transform +   │           │  Json2Parquet│
        │ Cleanse       │           │               │
        └──────────┬───┘           └────────┬──────┘
                   │                        │
                   └────────┬───────────────┘
                            │
                            ▼
                   ┌──────────────────────┐
                   │   Silver Layer       │
                   │ (Cleansed & Curated) │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │  DQ Lambda           │
                   │  (Quality Validation)│
                   └──────────┬───────────┘
                              │
                        ┌─────┴─────┐
                        │           │
                   ✅ PASS      ❌ FAIL
                        │           │
                        ▼           ▼
                   ┌────────┐   ┌──────────┐
                   │ Glue   │   │ SNS Alert│
                   │ Gold   │   └──────────┘
                   │ Layer  │
                   └────────┘
                        │
                        ▼
            ┌───────────────────────────┐
            │  Athena / QuickSight      │
            │  (Business Intelligence) │
            └───────────────────────────┘
```

### Medallion Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw Data Lake)                                            │
│ ├─ youtube/raw_statistics/region=US/date=2026-04-01/hour=12/*.json     │
│ └─ youtube/raw_statistics_reference_data/region=US/*.json               │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↓
              ┌──────────────────────────────────────┐
              │ PySpark ETL (Schema + Cleansing)     │
              └──────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleansed & Validated)                                     │
│ ├─ cleansed_statistics/region=US/*.parquet (deduped, typed, enriched)  │
│ └─ cleaned_reference/region=US/*.parquet (normalized categories)        │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↓
              ┌──────────────────────────────────────┐
              │ Data Quality Checks (DQ Lambda)      │
              │ • Row count ≥ 10                     │
              │ • Nulls ≤ 5%                         │
              │ • Schema validation                  │
              │ • Freshness < 48h                    │
              └──────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Business Aggregations)                                      │
│ ├─ trending_analytics/region=US/*.parquet (daily metrics)              │
│ ├─ channel_analytics/region=US/*.parquet (channel rankings)            │
│ └─ category_analytics/region=US/*.parquet (category performance)       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Infrastructure Components

| Layer | Service | Purpose |
|-------|---------|---------|
| Ingestion | Lambda + YouTube API | Fetch trending videos & categories |
| Storage | S3 (3 buckets) | Bronze/Silver/Gold data lake layers |
| Processing | AWS Glue (PySpark) | Distributed data transformations |
| Orchestration | Step Functions | Workflow coordination & error handling |
| Quality | Lambda + CloudWatch | Automated data validation gates |
| Catalog | Glue Data Catalog | Metadata management |
| Analytics | Athena + QuickSight | SQL queries & visualizations |
| Monitoring | SNS + CloudWatch | Alerts & operational metrics |

## 🚀 Quick Start

### Prerequisites
- AWS Account with admin access
- YouTube Data API v3 key ([Get it here](https://console.cloud.google.com/))
- AWS CLI configured (`aws configure`)
- Python 3.9+
- Terraform (optional, for IaC deployment)



### 1. Set Up AWS Infrastructure

#### Option A: Manual Setup
```bash
# Create S3 buckets
export AWS_REGION="us-east-1"
export ENV="dev"

aws s3 mb s3://yt-pipeline-bronze-${AWS_REGION}-${ENV}
aws s3 mb s3://yt-pipeline-silver-${AWS_REGION}-${ENV}
aws s3 mb s3://yt-pipeline-gold-${AWS_REGION}-${ENV}
aws s3 mb s3://yt-pipeline-scripts-${AWS_REGION}-${ENV}

# Create Glue databases
aws glue create-database --database-input "{\"Name\": \"yt_bronze_${ENV}\"}"
aws glue create-database --database-input "{\"Name\": \"yt_silver_${ENV}\"}"
aws glue create-database --database-input "{\"Name\": \"yt_gold_${ENV}\"}"

# Create SNS topic for alerts
aws sns create-topic --name yt-pipeline-alerts-${ENV}
aws sns subscribe --topic-arn <TOPIC_ARN> --protocol email --notification-endpoint your-email@example.com
```

#### Option B: Terraform (Recommended)
```bash
cd terraform
terraform init
terraform plan -var="environment=dev" -var="youtube_api_key=YOUR_API_KEY"
terraform apply -auto-approve
```

### 2. Deploy Lambda Functions
```bash
# Deploy ingestion Lambda
cd lambdas/youtube_api_integration
pip install -r requirements.txt -t .
zip -r function.zip .
aws lambda create-function \
  --function-name yt-ingestion-${ENV} \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables="{YOUTUBE_API_KEY=YOUR_KEY,S3_BUCKET_BRONZE=yt-pipeline-bronze-${AWS_REGION}-${ENV}}"

# Deploy DQ Lambda
cd ../../data_quality
# Repeat similar steps...
```

### 3. Deploy Glue Jobs
```bash
# Upload scripts to S3
aws s3 cp glue_jobs/bronze_to_silver_statistics.py s3://yt-pipeline-scripts-${AWS_REGION}-${ENV}/glue/
aws s3 cp glue_jobs/silver_to_gold_analytics.py s3://yt-pipeline-scripts-${AWS_REGION}-${ENV}/glue/

# Create Glue jobs
aws glue create-job \
  --name bronze-to-silver-${ENV} \
  --role arn:aws:iam::ACCOUNT_ID:role/glue-execution-role \
  --command "Name=glueetl,ScriptLocation=s3://yt-pipeline-scripts-${AWS_REGION}-${ENV}/glue/bronze_to_silver_statistics.py" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

### 4. Deploy Step Functions Workflow
```bash
aws stepfunctions create-state-machine \
  --name yt-pipeline-${ENV} \
  --definition file://step_functions/pipeline_orchestration.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/stepfunctions-execution-role
```

### 5. Configure Scheduled Execution
```bash
# Run every 6 hours
aws events put-rule \
  --name yt-pipeline-schedule \
  --schedule-expression "rate(6 hours)"

aws events put-targets \
  --rule yt-pipeline-schedule \
  --targets "Id=1,Arn=arn:aws:states:REGION:ACCOUNT_ID:stateMachine:yt-pipeline-${ENV},RoleArn=arn:aws:iam::ACCOUNT_ID:role/eventbridge-execution-role"
```

### 6. Run the Pipeline

#### Manual Execution
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:yt-pipeline-${ENV}
```

#### Monitor Execution
```bash
# Watch Step Functions console
https://console.aws.amazon.com/states/home?region=REGION#/statemachines

# Check CloudWatch logs
aws logs tail /aws/lambda/yt-ingestion-${ENV} --follow
```

## 📚 Documentation

### Pipeline Stages

<details>
<summary><strong>Stage 1: Data Ingestion (Bronze Layer)</strong></summary>

**Lambda Function:** `youtube_api_integration/lambda_function.py`

**What it does:**
- Fetches top 50 trending videos per region via YouTube Data API v3
- Retrieves category ID mappings for each region
- Stores raw JSON in S3 with partitioning: `region=XX/date=YYYY-MM-DD/hour=HH`

**Configuration:**
```python
YOUTUBE_REGIONS = ['US', 'GB', 'CA', 'DE', 'FR', 'IN', 'JP', 'KR', 'MX', 'RU']
MAX_RESULTS_PER_REGION = 50
```

**Output:**
```
s3://bronze-bucket/
  └─ youtube/
      ├─ raw_statistics/region=US/date=2026-04-01/hour=12/trending_123456.json
      └─ raw_statistics_reference_data/region=US/categories_20260401.json
```

</details>

<details>
<summary><strong>Stage 2: Data Cleansing (Silver Layer)</strong></summary>

**Glue Job:** `bronze_to_silver_statistics.py`

**Transformations:**
- **Schema Unification** — Handles both API JSON and Kaggle CSV formats
- **Type Casting** — Converts strings to proper data types (Long, Date, Boolean)
- **Deduplication** — Keeps latest record per (video_id, region, trending_date)
- **Derived Metrics:**
  ```python
  like_ratio = likes / views
  engagement_rate = (likes + dislikes + comment_count) / views
  ```
- **Null Handling** — Fills missing values with defaults (0 for numeric, 'Unknown' for strings)
- **Partitioning** — Outputs partitioned by region for efficient querying

**Output Schema:**
```
video_id: string
title: string
channel_title: string
category_id: long
views: long
likes: long
dislikes: long
comment_count: long
trending_date_parsed: date
like_ratio: double
engagement_rate: double
region: string
```

</details>

<details>
<summary><strong>Stage 3: Data Quality Validation</strong></summary>

**Lambda Function:** `data_quality/dq_lambda.py`

**Validation Checks:**

| Check | Threshold | Action on Fail |
|-------|-----------|----------------|
| Row Count | ≥ 10 rows | Halt pipeline, send SNS alert |
| Null Percentage | ≤ 5% on critical columns | Halt pipeline, send SNS alert |
| Schema Validation | All required columns present | Halt pipeline, send SNS alert |
| Value Ranges | Views > 0, Likes ≥ 0 | Halt pipeline, send SNS alert |
| Data Freshness | Latest data < 48 hours old | Warning alert, continue |

**Critical Columns:**
```python
CRITICAL_COLUMNS = [
    'video_id', 'title', 'channel_title', 'category_id',
    'views', 'likes', 'trending_date_parsed', 'region'
]
```

**SNS Alert Example:**
```
Subject: [ALERT] YouTube Pipeline - Data Quality Failed

Pipeline execution FAILED at Data Quality stage.

Failed Checks:
- Row count check: Expected ≥10, got 3
- Null percentage check: Column 'title' has 12% nulls (threshold: 5%)

Execution ARN: arn:aws:states:us-east-1:123456789:execution:yt-pipeline:abc123
Timestamp: 2026-04-01 14:23:45 UTC
```

</details>

<details>
<summary><strong>Stage 4: Business Aggregations (Gold Layer)</strong></summary>

**Glue Job:** `silver_to_gold_analytics.py`

**Output Tables:**

#### 1. trending_analytics
Daily aggregated metrics per region.

```sql
CREATE EXTERNAL TABLE trending_analytics (
    region STRING,
    trending_date_parsed DATE,
    total_videos BIGINT,
    total_views BIGINT,
    total_likes BIGINT,
    avg_views_per_video DOUBLE,
    avg_like_ratio DOUBLE,
    avg_engagement_rate DOUBLE,
    unique_channels BIGINT,
    unique_categories BIGINT
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3://gold-bucket/trending_analytics/';
```

**Sample Query:**
```sql
-- Get trending performance over time for US
SELECT 
    trending_date_parsed,
    total_videos,
    avg_views_per_video,
    avg_engagement_rate
FROM trending_analytics
WHERE region = 'US'
ORDER BY trending_date_parsed DESC
LIMIT 30;
```

#### 2. channel_analytics
Channel performance and rankings.

```sql
CREATE EXTERNAL TABLE channel_analytics (
    channel_title STRING,
    region STRING,
    total_videos BIGINT,
    total_views BIGINT,
    avg_engagement_rate DOUBLE,
    times_trending BIGINT,
    rank_in_region INT,
    categories STRING  -- Comma-separated list
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3://gold-bucket/channel_analytics/';
```

**Sample Query:**
```sql
-- Top 10 channels in India by total views
SELECT 
    channel_title,
    total_views,
    times_trending,
    rank_in_region
FROM channel_analytics
WHERE region = 'IN'
ORDER BY total_views DESC
LIMIT 10;
```

#### 3. category_analytics
Category-level performance with view share.

```sql
CREATE EXTERNAL TABLE category_analytics (
    category STRING,
    region STRING,
    trending_date_parsed DATE,
    video_count BIGINT,
    total_views BIGINT,
    avg_engagement_rate DOUBLE,
    view_share_pct DOUBLE
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3://gold-bucket/category_analytics/';
```

**Sample Query:**
```sql
-- Category distribution for Japan on latest date
SELECT 
    category,
    video_count,
    view_share_pct
FROM category_analytics
WHERE region = 'JP'
  AND trending_date_parsed = (SELECT MAX(trending_date_parsed) FROM category_analytics WHERE region = 'JP')
ORDER BY view_share_pct DESC;
```

</details>

### Configuration Reference

<details>
<summary><strong>Environment Variables</strong></summary>

**Ingestion Lambda:**
```bash
YOUTUBE_API_KEY=AIzaSy...              # Required
S3_BUCKET_BRONZE=yt-pipeline-bronze-*  # Required
YOUTUBE_REGIONS=US,GB,CA,DE,FR,IN,JP,KR,MX,RU  # Optional (default shown)
MAX_RESULTS_PER_REGION=50              # Optional
```

**Data Quality Lambda:**
```bash
S3_BUCKET_SILVER=yt-pipeline-silver-*  # Required
GLUE_DB_SILVER=yt_silver_dev           # Required
SNS_ALERT_TOPIC_ARN=arn:aws:sns:...    # Required
DQ_MIN_ROW_COUNT=10                    # Optional
DQ_MAX_NULL_PERCENT=5.0                # Optional
DQ_FRESHNESS_HOURS=48                  # Optional
```

**Glue Jobs:**
```bash
--bronze_database=yt_bronze_dev
--bronze_table=raw_statistics
--silver_database=yt_silver_dev
--silver_bucket=s3://yt-pipeline-silver-us-east-1-dev
--gold_database=yt_gold_dev
--gold_bucket=s3://yt-pipeline-gold-us-east-1-dev
```

</details>

<details>
<summary><strong>Step Functions Workflow</strong></summary>

**State Machine Definition:** `step_functions/pipeline_orchestration.json`

**Workflow Steps:**
1. IngestData (Lambda) → Fetch YouTube data
2. Wait (5 minutes) → Allow S3 consistency
3. ParallelTransform (Parallel state):
   - TransformStatistics (Glue Job)
   - TransformReference (Lambda)
4. DataQualityCheck (Lambda) → Validation gate
5. GoldAggregation (Glue Job) → Business analytics
6. SendSuccessNotification (SNS)

**Error Handling:**
- **Retry Policy:** 3 attempts with exponential backoff (1s, 2s, 4s)
- **Catch Policy:** Send SNS alert with error details
- **Timeout:** 2 hours for entire pipeline

**Sample Execution Input:**
```json
{
  "execution_date": "2026-04-01",
  "force_refresh": false,
  "regions": ["US", "GB", "IN"]
}
```

</details>

## 🎯 Use Cases

### 1. Content Strategy Analysis
Identify trending topics and optimal posting times by analyzing:
- Peak trending hours across regions
- Category performance trends
- Viral content characteristics

**Query Example:**
```sql
-- Find best-performing categories by engagement
SELECT 
    category,
    AVG(avg_engagement_rate) as avg_engagement,
    COUNT(DISTINCT trending_date_parsed) as days_trending
FROM category_analytics
WHERE region = 'US'
GROUP BY category
ORDER BY avg_engagement DESC;
```

### 2. Competitive Benchmarking
Track competitor channel performance:

```sql
-- Compare your channel against competitors
WITH competitor_channels AS (
    SELECT 'YourChannel' as channel UNION ALL
    SELECT 'Competitor1' UNION ALL
    SELECT 'Competitor2'
)
SELECT 
    c.channel_title,
    c.total_videos,
    c.total_views,
    c.rank_in_region
FROM channel_analytics c
WHERE c.channel_title IN (SELECT channel FROM competitor_channels)
  AND c.region = 'US'
ORDER BY c.total_views DESC;
```

### 3. Regional Market Analysis
Understand content preferences across geographies:

```sql
-- Compare category preferences across regions
SELECT 
    region,
    category,
    SUM(total_views) as total_views
FROM category_analytics
GROUP BY region, category
ORDER BY region, total_views DESC;
```

### 4. Trend Forecasting
Build predictive models using historical trending data:
- Export Gold tables to SageMaker
- Train models on trending patterns
- Predict future viral content

## 🧪 Testing

### Unit Tests
```bash
# Test Lambda functions
cd lambdas/youtube_api_integration
python -m pytest tests/ -v

# Test Glue jobs locally
cd glue_jobs
spark-submit --master local[*] bronze_to_silver_statistics.py \
  --bronze_database test_bronze \
  --silver_bucket s3://test-silver-bucket
```

### Integration Tests
```bash
# Run end-to-end pipeline test
cd tests
python test_pipeline_e2e.py --environment test
```

### Data Quality Tests
```bash
# Validate Gold layer output
cd tests
python test_gold_layer_quality.py --region US --date 2026-04-01
```

## 📊 Demo

### Sample Athena Queries

```sql
-- 1. Top 10 trending videos of all time
SELECT 
    title,
    channel_title,
    views,
    trending_date_parsed,
    region
FROM yt_silver_dev.cleansed_statistics
ORDER BY views DESC
LIMIT 10;

-- 2. Channel growth over time
SELECT 
    trending_date_parsed,
    channel_title,
    COUNT(DISTINCT video_id) as daily_trending_videos,
    SUM(views) as total_views
FROM yt_silver_dev.cleansed_statistics
WHERE channel_title = 'MrBeast'
GROUP BY trending_date_parsed, channel_title
ORDER BY trending_date_parsed;

-- 3. Category trends by region
SELECT 
    region,
    category,
    AVG(view_share_pct) as avg_view_share,
    AVG(avg_engagement_rate) as avg_engagement
FROM yt_gold_dev.category_analytics
GROUP BY region, category
ORDER BY region, avg_view_share DESC;
```

### QuickSight Dashboard
Connect QuickSight to the Gold Athena tables to create visualizations:

**Trending Metrics Dashboard:**
- Daily trending video counts (line chart)
- Top channels by views (bar chart)
- Category distribution (pie chart)

**Regional Comparison Dashboard:**
- Engagement rates by region (heat map)
- Cross-region category preferences (stacked bar chart)

**Channel Performance Dashboard:**
- Channel ranking trends (area chart)
- Videos trending over time (timeline)

## 🔧 Troubleshooting

<details>
<summary><strong>Pipeline Failures</strong></summary>

**Issue:** Step Functions execution fails at Ingestion stage

**Solution:**
```bash
# Check Lambda logs
aws logs tail /aws/lambda/yt-ingestion-dev --follow

# Verify YouTube API key
aws lambda get-function-configuration --function-name yt-ingestion-dev \
  | jq '.Environment.Variables.YOUTUBE_API_KEY'

# Test API manually
curl "https://www.googleapis.com/youtube/v3/videos?part=snippet&chart=mostPopular&regionCode=US&key=YOUR_KEY"
```

</details>

<details>
<summary><strong>Data Quality Failures</strong></summary>

**Issue:** DQ Lambda halts pipeline due to null threshold

**Solution:**
```bash
# Inspect Silver data
aws s3 ls s3://yt-pipeline-silver-us-east-1-dev/cleansed_statistics/region=US/ --recursive

# Query via Athena
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as title_null_pct
FROM yt_silver_dev.cleansed_statistics
WHERE region = 'US';

# Adjust threshold if needed
aws lambda update-function-configuration \
  --function-name yt-dq-dev \
  --environment Variables="{DQ_MAX_NULL_PERCENT=10.0}"
```

</details>

<details>
<summary><strong>Glue Job Errors</strong></summary>

**Issue:** Bronze-to-Silver Glue job fails with schema error

**Solution:**
```bash
# View job run logs
aws glue get-job-run --job-name bronze-to-silver-dev --run-id jr_xxx

# Check CloudWatch Logs
aws logs tail /aws-glue/jobs/output --follow

# Test locally with sample data
spark-submit --master local[*] glue_jobs/bronze_to_silver_statistics.py \
  --bronze_database yt_bronze_dev \
  --bronze_table raw_statistics \
  --silver_bucket s3://test-bucket
```

</details>

## 🚀 Performance Optimization

### Cost Optimization

| Strategy | Savings | Implementation |
|----------|---------|-----------------|
| S3 Intelligent Tiering | 30-40% | Enable on Bronze bucket after 30 days |
| Glue job auto-scaling | 20-30% | Use MaxCapacity instead of fixed workers |
| Athena query result caching | 50%+ | Enable in workgroup settings |
| Parquet compression | 80% | Already implemented (Snappy) |

```bash
# Enable S3 Intelligent Tiering
aws s3api put-bucket-intelligent-tiering-configuration \
  --bucket yt-pipeline-bronze-us-east-1-dev \
  --id IntelligentTieringConfig \
  --intelligent-tiering-configuration file://s3-tiering-config.json
```

### Query Performance

```sql
-- Use partitions in WHERE clause
SELECT * FROM yt_gold_dev.trending_analytics
WHERE region = 'US'  -- Partition pruning
  AND trending_date_parsed >= DATE '2026-01-01';

-- Avoid SELECT *
SELECT video_id, title, views  -- Only needed columns
FROM yt_silver_dev.cleansed_statistics
LIMIT 100;

-- Use CTAS for complex queries
CREATE TABLE daily_aggregates
WITH (format = 'PARQUET', parquet_compression = 'SNAPPY') AS
SELECT region, trending_date_parsed, SUM(views) as total_views
FROM yt_silver_dev.cleansed_statistics
GROUP BY region, trending_date_parsed;
```


