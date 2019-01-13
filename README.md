A set of examples for streaming data processing using Apache Spark, Twitter source and Apache Kafka as data producer.
ElasticSearch and Kibana are used to store and display the data (respectively).

The project consists of 2 parts:

1) Legacy streaming
Simple data flow from data producer (Kafka) to consumer (Spark). The data format is simple String which is further stored in ES.
Further implementation plans:
Replace String with JSON format

2) Structured streaming
The same flow from Twitter to Kafka and Spark. ELK stack is not used, the data is displayed on the console format (for now). Fields to be discovered:
- Filtering and aggregations on data (implemented)
- Windowed aggregations (in process)
- Watermarking
- Metrics gathering

Future plans:
- Upgrade Spark to 2.4.0
- Replace Kafka producer String format with JSON format for the legacy Spark streaming case
- ELK stack introduction for Spark structured streaming case
- Deploy the project on Kubernetes

Spark 2.3.1 is used for now. Scala version is current release (2.12.8)
