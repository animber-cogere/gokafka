# data-transform Go Project

The goal of this project is to read in, transform, and store json data using Apache Kafka Producer/Consumer architecture.

- Producer: Read the JSON from an HTTPS endpoint as a stream, process it incrementally, and send chunks to Kafka.
- Consumers: Read messages from Kafka and write them to a CSV file.

Terminate gracefully when receiving a shutdown message.

## Table of Contents

- [Test Environment](#test-environment)
- [Configuration](#configuration)
  - [Start the Kafka Broker](#start-the-kafka-broker)
  - [Build the producer and consumer](#build-the-producer-and-consumer)
- [Usage](#usage)
  - [Parsing Command-Line Arguments](#parsing-command-line-arguments)
  - [Example](#example)

## Test Environment

- Go
- Kafka broker
- Confluent Kafka Go client library (Optional)

## Configuration

### Start the Kafka Broker

```bash
$ confluent local kafka start
```

*Note the **Plaintext Ports** printed in your terminal, which you will use when configuring the producer and consumer clients in upcoming steps.*

### Build the producer and consumer

```bash
$ go build producer.go
$ go build consumer.go
```

## Usage

### Parsing Command-Line Arguments

The project uses command-line arguments to configure various settings such as Kafka broker URL, topic name, and the number of consumers. You can provide these arguments when running the binary.

***Producer***
- **-url:** The endpoint from which the data will be fetched. Default: https://open.gsa.gov/data.json
- **-kafka-broker** The URL of the Kafka broker. Default: ***localhost:9092***
- **kafka-topic:** The name of the Kafka topic. Default: ***data-transform***
- **-partitions:** The number of partitions. Default: ***8***
- **-repl-factor:** The replication factor as per your Kafka setup. Default: ***1***

***Consumer***
- **-kafka-broker:** The URL of the Kafka broker. Default: ***localhost:9092***
- **-kafka-topic:** The name of the Kafka topic. Default: ***data-transform***
- **-group-id:** The consumer group ID. Default: ***data-transform-group***
- **-consumers:** The number of consumers. Default: ***8***
- **-output:** The name of the output CSV file for transformed data. Default: ***output.csv***

### Example
```bash
./consumer -kafka-broker=localhost:9092 -kafka-topic=data-transform -group-id=data-transform-group -consumers=8 -output=output.csv

./producer -url=https://open.gsa.gov/data.json -kafka-broker=localhost:9092 -kafka-topic=data-transform -partitions=8 -repl-factor=1
```

Simply, run

```bash
$ ./consumer
$ ./producer 
```

The two programs can be run independently, and it is recommended to run them on different consoles.
