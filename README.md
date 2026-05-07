# Real-Time Cryptocurrency Market Data Pipeline

A production-grade real-time data pipeline for cryptocurrency market analytics built with Apache Kafka, Apache Spark Structured Streaming, and AWS services.

## Project Overview

This project implements a comprehensive real-time cryptocurrency market data pipeline that ingests live trade data from the Coinbase Advanced Trade WebSocket API, processes it through Apache Kafka and Spark, and stores results in DynamoDB and Amazon S3. The system computes one-minute Open-High-Low-Close-Volume (OHLCV) candlestick bars and Volume-Weighted Average Price (VWAP) aggregations for Bitcoin (BTC-USD), Ethereum (ETH-USD), and Solana (SOL-USD).

## Authors

- **Aung Kham Naung** (A20491106) - Data Ingestion & Kafka Producer
- **Akshitha P. Murugan** (A20601120) - Spark Structured Stream Processing
- **Fino Franklin John Bosco** (A20580114) - Storage: DynamoDB & S3
- **Eric Mattner** (A20483729) - Data Quality & Profiling
- **Rudraksha R. Kokane** (A20596373) - Integration, Testing & Dashboard

**Course:** CSP 554 – Big Data Technology  
**Instructor:** Professor Joseph Rosen  
**Submission Date:** May 6, 2026

## System Architecture

The pipeline follows a five-stage architecture:

```
Coinbase API → Kafka (MSK) → Spark (EMR) → DynamoDB + S3 → Dashboard
```

### Components

1. **Data Ingestion**: WebSocket producer consuming trades from Coinbase Advanced Trade API
2. **Message Broker**: AWS Managed Streaming for Apache Kafka (MSK)
3. **Stream Processing**: Apache Spark Structured Streaming on AWS EMR
4. **Storage**: Amazon DynamoDB (real-time) and Amazon S3 (archival in Parquet format)
5. **Visualization**: Flask web application with Chart.js dashboard

## Performance Metrics

- **Throughput**: 58,344 trades per minute
- **Median End-to-End Latency**: 124 ms
- **P99 Latency**: 987 ms
- **Data Quality Score**: 99.8%
- **Duplicate Rate**: 0.195%
- **Field Completeness**: 100%

## Prerequisites

- Python 3.8 or higher
- AWS Account with appropriate IAM permissions
- AWS CLI configured with credentials
- Docker (optional, for containerized deployment)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/aungkham-naung/csp-554.git
cd csp-554
```

### 2. Set Up Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### Required Packages

- `confluent-kafka==2.14.0` - Kafka Python client
- `boto3==1.43.3` - AWS SDK for Python
- `pyspark==4.1.1` - Apache Spark
- `flask==3.1.3` - Web framework for dashboard
- `websocket-client==1.9.0` - WebSocket client for Coinbase API

## Configuration

### Environment Variables

Create a `.env` file in the project root or set the following environment variables:

```bash
# AWS Configuration
export AWS_REGION=us-east-2

# Kafka Configuration (MSK)
export KAFKA_BOOTSTRAP=<MSK_BROKER_ENDPOINT>:9092
export KAFKA_TOPIC=crypto-trades

# Kinesis Configuration (Alternative)
export KINESIS_STREAM=crypto-trades-kinesis

# DynamoDB Configuration
export DYNAMODB_TABLE=CryptoMetrics

# S3 Configuration
export S3_BUCKET=csp544-crypto-pipeline

# Data Processing
export DATA_FILE=captured_trades.jsonl
export REPLAY_SPEED=20.0
```

### AWS IAM Permissions

Ensure your IAM user/role has permissions for:

- MSK (or Kinesis) cluster access
- DynamoDB read/write
- S3 bucket read/write
- EMR cluster management (if using managed Spark)

See `iam/` directory for example policies.

## Project Structure

```
csp-554/
├── producer/                    # Data ingestion modules
│   ├── kafka_producer.py       # Coinbase WebSocket → Kafka
│   ├── kinesis_replay_producer.py  # Replay from file → Kinesis
│   ├── replay_producer.py      # Replay from file → Kafka
│   └── create_topic.py         # Kafka topic creation utility
│
├── spark/                       # Stream processing jobs
│   ├── streaming_job.py        # Spark Structured Streaming (Kafka)
│   └── streaming_job_kinesis.py # Spark Structured Streaming (Kinesis)
│
├── dashboard/                   # Web visualization
│   └── web_dashboard.py        # Flask API and Chart.js interface
│
├── capture/                     # Data capture utilities
│   └── ws_capture.py           # Standalone WebSocket capture
│
├── profiling/                   # Data quality analysis
│   └── data_profiler.py        # Spark-based data profiling
│
├── iam/                         # AWS IAM policy examples
│   ├── emr_policy.json
│   ├── emr_policy_kinesis.json
│   └── dashboard_policy.json
│
├── data/                        # Data directory (local caches, captured trades)
│   └── *.jsonl                 # Captured trade records
│
├── apache+spark/               # Spark event logs and artifacts
├── kinesis/                    # Kinesis-based event logs
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore rules
└── CSP544_Final_Report.docx   # Complete project report

```

## Usage

### Option 1: Kafka Pipeline

#### Step 1: Start Kafka Producer

```bash
export KAFKA_BOOTSTRAP=<your-msk-broker>:9092
python3 producer/kafka_producer.py
```

This connects to Coinbase Advanced Trade API and streams trades to the Kafka topic.

#### Step 2: Start Spark Streaming Job

Submit to EMR cluster:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4G \
  --executor-cores 2 \
  --driver-memory 2G \
  -c spark.dynamodb.throughput.read_percent=0.5 \
  -c spark.dynamodb.throughput.write_percent=1.0 \
  spark/streaming_job.py
```

Or run locally for development:

```bash
export KAFKA_BOOTSTRAP=localhost:9092
python3 spark/streaming_job.py
```

#### Step 3: Launch Dashboard

```bash
export DYNAMODB_TABLE=CryptoMetrics
python3 dashboard/web_dashboard.py
```

Open browser to `http://localhost:5050`

### Option 2: Kinesis Pipeline

#### Step 1: Replay Data to Kinesis

```bash
export KINESIS_STREAM=crypto-trades-kinesis
python3 producer/kinesis_replay_producer.py
```

#### Step 2: Start Kinesis Spark Job

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  spark/streaming_job_kinesis.py
```

### Capture Live Data for Replay

```bash
python3 capture/ws_capture.py
# Saves to data/captured_trades.jsonl
```

### Run Data Profiling Analysis

```bash
python3 profiling/data_profiler.py
```

Generates data quality metrics including:
- Duplicate rate
- Field completeness
- Value distributions
- Anomaly detection

## Data Formats

### Input Format (Coinbase WebSocket)

```json
{
  "type": "match",
  "product_id": "BTC-USD",
  "price": "45678.50",
  "size": "0.5",
  "side": "buy",
  "trade_id": 123456789,
  "time": "2026-05-06T14:30:45.123Z"
}
```

### Processing Format (Kafka/Kinesis)

```json
{
  "symbol": "BTCUSD",
  "price": 45678.50,
  "quantity": 0.5,
  "trade_time": 1715000445123,
  "buyer_maker": false,
  "trade_id": 123456789
}
```

### Output Format (DynamoDB/S3)

```json
{
  "symbol": "BTCUSD",
  "window_start": "2026-05-06T14:30:00Z",
  "window_end": "2026-05-06T14:31:00Z",
  "open": 45600.50,
  "high": 45750.00,
  "low": 45500.25,
  "close": 45678.50,
  "volume": 125.43,
  "trade_count": 847,
  "vwap": 45645.32,
  "avg_price": 45650.12
}
```

## Key Features

### Stateful Stream Processing

- One-minute tumbling windows for OHLC aggregation
- Event time semantics with 30-second watermark
- Automatic late-data handling

### Multi-Output Pipeline

- **Real-Time**: DynamoDB for sub-second dashboard queries
- **Batch**: S3 Parquet for historical analysis
- **Monitoring**: Console output for pipeline health

### Fault Tolerance

- Kafka checkpointing for exactly-once semantics
- Idempotent producer configuration
- DynamoDB batch write with retry logic
- S3 checkpoint storage for recovery

### Data Quality

- Duplicate detection and metrics
- Schema validation
- Field completeness checks
- Outlier detection for anomalies

## Deployment

### AWS EMR Deployment

1. Create EMR cluster with Spark and Hadoop
2. Upload project code to S3
3. Submit Spark job:

```bash
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps Type=spark,Name='CryptoStreamProcessor',SparkSubmitParameters='--master yarn --deploy-mode cluster spark/streaming_job.py'
```

### Docker Deployment (Optional)

```bash
docker build -t crypto-pipeline .
docker run -e AWS_REGION=us-east-2 -e KAFKA_BOOTSTRAP=<host> crypto-pipeline
```

## Monitoring and Troubleshooting

### Kafka Topic Monitoring

```bash
kafka-console-consumer.sh \
  --bootstrap-server <MSK_BROKER>:9092 \
  --topic crypto-trades \
  --from-beginning \
  --max-messages 10
```

### Spark Streaming Metrics

Monitor via:
- EMR Hadoop Dashboard: `http://master-node:8088`
- Spark History Server: `http://master-node:18080`
- Event logs in `apache+spark/` and `kinesis/` directories

### DynamoDB Query Performance

```bash
aws dynamodb query \
  --table-name CryptoMetrics \
  --key-condition-expression "symbol = :sym" \
  --expression-attribute-values '{":sym": {"S": "BTCUSD"}}'
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Kafka broker connection timeout | Verify MSK security groups allow port 9092 |
| DynamoDB write throttling | Increase provisioned capacity or enable on-demand billing |
| Spark OOM errors | Increase executor memory with `--executor-memory` |
| Missing Coinbase data | Verify WebSocket API credentials and firewall rules |

## Testing

Run unit tests (if applicable):

```bash
python3 -m pytest tests/
```

Validate Spark job locally:

```bash
spark-submit --master local[2] spark/streaming_job.py
```

## Documentation

Complete technical documentation, architecture diagrams, and performance analysis available in:

- `CSP544_Final_Report.docx` - Full project report with design decisions and evaluation
- GitHub Repository: https://github.com/aungkham-naung/csp-554

## Dataset and Results

The project includes:

- Captured cryptocurrency trades (`data/captured_trades.jsonl`)
- Spark event logs with detailed execution metrics
- Data quality profiling results
- Performance benchmarks and latency analysis

## License

Academic project for CSP 554 course. See repository for full details.

## References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- AWS MSK Documentation: https://docs.aws.amazon.com/msk/
- AWS EMR Documentation: https://docs.aws.amazon.com/emr/
- Coinbase Advanced Trade API: https://docs.cloud.coinbase.com/advanced-trade/docs

## Contact

For questions regarding this project, please refer to the final report or contact the course instructor.
