# Streaming Social Media Analytics

An end-to-end streaming analytics pipeline built with Apache Kafka, Apache Spark, Python, Docker, and Google Cloud Platform (GCP).

This project processes social media posts as a continuous stream, identifies configured people and organizations mentioned in the text, performs sentiment analysis, and converts those co-mentions into structured relationship data that can be analyzed over time.

The project was developed for **CSc 84030 - Big Data Analytics** at the **CUNY Graduate Center**.

---

## Overview

Social media platforms generate large amounts of continuously changing, unstructured text. This project explores how distributed streaming technologies can transform that data into structured analytics in near real time.

The system:

1. Ingests social media records into Apache Kafka.
2. Processes incoming records with Spark Structured Streaming.
3. Detects configured people and companies mentioned in each post.
4. Applies VADER sentiment analysis to the text.
5. Creates relationship edges between co-mentioned entities.
6. Stores processed results in Apache Parquet.
7. Aggregates relationship statistics for offline analysis.
8. Uses Spark checkpointing and Kafka persistence for recovery.

The result is a continuously updated dataset describing how selected people and organizations are discussed in relation to one another.

---

## Architecture

```text
Social Media Data
       |
       v
 Python Producer
       |
       v
 Apache Kafka
       |
       v
Spark Structured Streaming
       |
       +--> Entity Matching
       |
       +--> VADER Sentiment Analysis
       |
       v
Relationship Edge Records
       |
       v
 Apache Parquet
       |
       v
Spark Batch Aggregation
       |
       v
Relationship Statistics
```

The infrastructure is containerized with Docker Compose and was deployed and evaluated on Google Cloud Platform.

---

## Example Relationship Record

Each processed relationship contains information such as:

```text
source entity
destination entity
tweet/post ID
timestamp
sentiment score
sentiment label
```

For example:

```text
Elon Musk -> Tesla
sentiment_score = 0.62
sentiment_label = positive
```

Multiple records can then be aggregated to calculate:

- Number of co-mentions
- Average sentiment
- Sentiment standard deviation
- First observed timestamp
- Most recent observed timestamp

---

## Tech Stack

- **Python**
- **Apache Kafka**
- **Apache Spark**
- **Spark Structured Streaming**
- **PySpark**
- **Apache Parquet**
- **Docker**
- **Docker Compose**
- **Google Cloud Platform**
- **VADER Sentiment Analysis**
- **YAML**

---

## Key Components

### Kafka Producer

The producer normalizes incoming records into a consistent JSON format and publishes them to a Kafka topic.

The project supports:

- File-based replay for repeatable experiments
- Twitter/X recent-search API ingestion

File replay was primarily used for testing because Twitter/X API access limitations made large-scale live API experiments impractical.

---

### Entity Matching

Entities are defined through a YAML configuration file.

Each entity contains:

- Canonical name
- Matching patterns
- Entity type
- Country
- Tags

Example:

```yaml
- canonical_name: "Elon Musk"
  patterns:
    - "elon musk"
    - "@elonmusk"
  type: "person"
  country: "US"
  tags:
    - "billionaire"
    - "tech"
    - "ceo"
```

Posts containing multiple configured entities generate relationship edges between those entities.

---

### Sentiment Analysis

The pipeline uses **VADER (Valence Aware Dictionary and sEntiment Reasoner)** to assign a compound sentiment score to each post.

Scores are classified as:

```text
Positive: compound >= 0.05
Negative: compound <= -0.05
Neutral:  otherwise
```

The resulting sentiment score becomes part of the relationship edge.

---

### Spark Structured Streaming

Spark consumes records from Kafka and performs the primary streaming transformations.

The processing pipeline:

```text
Kafka
  -> JSON parsing
  -> entity matching
  -> sentiment analysis
  -> relationship edge generation
  -> Parquet output
```

Spark checkpointing is used to preserve streaming progress and support recovery after interruptions.

---

### Batch Analytics

A separate Spark job reads the generated Parquet relationship records and groups them by entity pair.

For every relationship, it calculates:

```text
count
average sentiment
sentiment standard deviation
first seen
last seen
```

This separates real-time data processing from later analytical queries.

---

## Performance Evaluation

The system was evaluated using replayed social media data to simulate a continuous high-volume stream.

During stress testing, the pipeline achieved approximately:

```text
Average input rate:      ~3,200 records/sec
Average processing rate: ~4,500 records/sec
Peak processing rate:    ~4,700 records/sec
Kafka consumer lag:      0 during the measured run
```

Spark was the primary compute consumer while Kafka remained relatively lightweight.

The experiments also evaluated:

- CPU usage
- Memory usage
- Streaming throughput
- Processing latency
- Kafka lag
- Checkpoint recovery
- Output growth

---

## Fault Recovery

The project includes a recovery experiment where processing is interrupted while data continues entering Kafka.

After restarting Spark with the existing checkpoint:

- Kafka retains unprocessed messages.
- Spark restores streaming progress from its checkpoint.
- Processing resumes from the previous state.
- Previously completed work does not need to be processed from the beginning.

This demonstrates the role of durable ingestion and checkpointing in streaming system reliability.

---

## Docker Infrastructure

Docker Compose is used to run the core services:

```text
ZooKeeper
Kafka
Spark Master
Spark Worker
```

Containerization keeps ingestion and processing components separated and makes the development environment reproducible.

---

## Data Source

Large-scale testing used the **Sentiment140** dataset.

The dataset contains approximately **1.6 million tweets** and was replayed through Kafka to simulate an incoming social media stream.

The original design also supports Twitter/X API ingestion, but API access restrictions made dataset replay more practical for sustained experiments.

---

## Limitations

This project is a proof-of-concept rather than a production social media analytics platform.

Current limitations include:

- Entity recognition uses configured string patterns rather than a full named-entity recognition model.
- VADER has limited understanding of sarcasm, memes, slang, and contextual meaning.
- Testing was performed on a relatively small cloud deployment rather than a large distributed cluster.
- Twitter/X API restrictions limited large-scale live-data collection.
- The entity catalog is manually configured.
- The project generates graph-like relationship data but does not currently perform advanced graph algorithms such as PageRank or community detection.

---

## Future Work

Possible extensions include:

- Dynamic entity discovery
- Named-entity recognition using NLP models
- More advanced sentiment models
- Twitter/social-media-specific sentiment fine-tuning
- Multiple Kafka partitions
- Multi-worker Spark deployments
- Larger distributed cloud clusters
- Live monitoring dashboards
- Interactive graph visualization
- Graph analytics such as centrality and community detection
- Additional social media data sources

---

## Project Goals

This project was designed to explore several concepts in big data systems:

- Stream processing
- Distributed data pipelines
- Separation of ingestion and computation
- Structured processing of unstructured data
- Fault recovery
- Cloud deployment
- Performance measurement
- Batch vs. streaming analytics
- Relationship-oriented data modeling

---

## Author

**Angel Coronel**

Computer Science / Software Engineering

Developed as a semester project for:

**CSc 84030 - Big Data Analytics**  
CUNY Graduate Center

---

## Project Documentation

Additional documentation from the project includes:

- Project proposal
- Technical presentation
- Final IEEE-style project report
- Experimental results and performance measurements
