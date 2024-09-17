package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// The endpoint from which the data will be fetched
	url = "https://open.gsa.gov/data.json"

	// Specifies the address of the Kafka broker
	kafkaBroker = "localhost:9092"

	// The name of the Kafka topic to which messages will be published
	kafkaTopic = "data-transform"

	// Defines the number of partitions for the Kafka topic
	numPartitions = 8

	// Sets the replication factor for the Kafka topic
	replicationFactor = 1
)

// StringOrSlice represents a JSON field that can be either a string or a slice of strings
type StringOrSlice []string

// UnmarshalJSON custom unmarshal method to handle both string and []string types
func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	// Try to unmarshal data as a string
	var singleString string
	if err := json.Unmarshal(data, &singleString); err == nil {
		*s = []string{singleString}
		return nil
	}

	// Try to unmarshal data as a slice of strings
	var stringSlice []string
	if err := json.Unmarshal(data, &stringSlice); err == nil {
		*s = stringSlice
		return nil
	}

	return fmt.Errorf("failed to unmarshal StringOrSlice")
}

// Structure that holds information about a sub-organization
// Contains a single field 'Name' which can be either a string or a slice of strings
type SubOrganizationOf struct {
	Name StringOrSlice `json:"name"`
}

// Represents the contact information of an entity
// The `fn` field can be either a string or a slice of strings to handle multiple contact names
type ContactPoint struct {
	Fn StringOrSlice `json:"fn"`
}

// Represents the publisher information of a dataset or document
// It includes the name of the publisher and details about the sub-organization
type Publisher struct {
	Name              StringOrSlice     `json:"name"`
	SubOrganizationOf SubOrganizationOf `json:"subOrganizationOf"`
}

// Represents the metadata for a dataset
// It includes information about when the dataset was modified, the publisher, contact points, and associated keywords
type Dataset struct {
	Modified     StringOrSlice `json:"modified"`
	Publisher    Publisher     `json:"publisher"`
	ContactPoint ContactPoint  `json:"contactPoint"`
	Keyword      StringOrSlice `json:"keyword"`
}

// Entry point of the application
func main() {
	// Parse the command-line arguments
	parseArgs()

	// Create a Kafka topic
	if err := createKafkaTopic(); err != nil {
		return
	}

	// Initialize a new Kafka producer with the specified configuration
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker, // Set the Kafka broker address
		"acks":              "all",       // Ensure all brokers acknowledge the message
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}
	defer producer.Close() // Ensure the producer is closed after use

	// Perform the HTTPS GET request to the specified URL
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to make request: %v\n", err)
		return
	}
	defer resp.Body.Close() // Ensure the response body is closed after use

	// Check for a successful response status code
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Received error status code %d\n", resp.StatusCode)
		return
	}

	// Make a signal channel for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create a JSON decoder for the response body
	decoder := json.NewDecoder(resp.Body)

	// Find the first dataset
	if err := findDataset(decoder); err != nil {
		fmt.Printf("Error reading dataset array start: %s\n", err)
		return
	}

	// Iterate over each dataset in the catalog
	run := true
	for decoder.More() && run {
		select {
		case <-sigchan:
			run = false
		default:
			var dataset Dataset
			err := decoder.Decode(&dataset)
			if err != nil {
				fmt.Printf("Error decoding dataset item: %s\n", err)
				continue
			}

			// print a dataset
			printDataset(&dataset)

			// Serialize dataset into a message
			serializedMessage, err := serializeMessage(&dataset)
			if err != nil {
				fmt.Printf("Failed to serialize message: %s\n", err)
				continue
			}

			// Produce the serialized message to the specified Kafka topic
			if err = produceMessage(producer, kafkaTopic, serializedMessage); err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
			}
		}
	}

	// Graceful shutdown, wait for all messages to be delivered before exiting
	producer.Flush(5000)
}

// Parse command-line arguments
func parseArgs() {
	// Define a flag for the URL of the data source
	url_ := flag.String(
		"url", url, "The URL of the data source")

	// Define a flag for the URL of the Kafka broker
	kafkaBroker_ := flag.String(
		"kafka-broker", kafkaBroker, "The URL of the Kafka broker")

	// Define a flag for the Kafka topic
	kafkaTopic_ := flag.String(
		"kafka-topic", kafkaTopic, "The Kafka topic")

	// Define a flag for the number of partitions
	numPartitions_ := flag.Int(
		"partitions", numPartitions, "The number of partitions")

	// Define a flag for the replication factor as per your Kafka setup
	replicationFactor_ := flag.Int(
		"repl-factor", replicationFactor, "The replication factor as per your Kafka setup")

	// Parse the command-line flags
	flag.Parse()

	// Assign the parsed flag values to the global variables
	url = *url_
	kafkaBroker = *kafkaBroker_
	kafkaTopic = *kafkaTopic_
	numPartitions = *numPartitions_
	replicationFactor = *replicationFactor_
}

// Create a Kafka topic
func createKafkaTopic() error {
	// Initialize a new admin client with the specified Kafka broker
	adminClient, err := kafka.NewAdminClient(
		&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		fmt.Printf("Error creating admin client: %v\n", err)
		return err
	}

	defer adminClient.Close() // Ensure the admin client is closed after use

	// Define the topic specification with the topic name, number of partitions, and replication factor
	topicSpec := kafka.TopicSpecification{
		Topic:             kafkaTopic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	// Create a context to control the lifetime of the topic creation request
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure the context is cancelled after use

	// Make a request to create the topics as per the specification
	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		fmt.Printf("Error creating topic: %v\n", err)
		return err
	}

	// Iterate over the results of the topic creation request
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			fmt.Printf("Created a topic %s: %v\n", result.Topic, result.Error)
		} else {
			fmt.Printf("Topic %s created successfully with partitions: %v\n", result.Topic, numPartitions)
		}
	}

	return nil
}

// Find the first dataset
func findDataset(decoder *json.Decoder) error {
	// Skip tokens until the start of the "dataset" array is found
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		// Check for the start of the "dataset" array
		if token == "dataset" {
			// Read the opening delimiter of the dataset array
			_, err = decoder.Token()
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
}

// Print the details of a dataset
func printDataset(dataset *Dataset) {
	fmt.Printf("\nmodified: %s\n", dataset.Modified)
	fmt.Printf("publisher.name: %s\n", dataset.Publisher.Name)
	fmt.Printf("publisher.subOrganizationOf.name: %s\n", dataset.Publisher.SubOrganizationOf.Name)
	fmt.Printf("contactPoint.fn: %s\n", dataset.ContactPoint.Fn)
	fmt.Printf("keyword: %s\n", dataset.Keyword)
}

// Serialize a message map to JSON format
func serializeMessage(dataset *Dataset) ([]byte, error) {
	// Map data to be serialized
	data := map[string][]string{
		"modified":                         dataset.Modified,
		"publisher.name":                   dataset.Publisher.Name,
		"publisher.subOrganizationOf.name": dataset.Publisher.SubOrganizationOf.Name,
		"contactPoint.fn":                  dataset.ContactPoint.Fn,
		"keyword":                          dataset.Keyword,
	}

	// Replace nil values in the map with empty string slices
	for key, val := range data {
		if val == nil {
			data[key] = []string{""}
		}
	}

	// Serialize the message struct to JSON
	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}
	return serialized, nil
}

// Produce a message to a Kafka topic using the provided producer
func produceMessage(producer *kafka.Producer, topic string, message []byte) error {
	// Create a new Kafka message to be produced
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}

	// Create a delivery channel to receive delivery reports
	deliveryChan := make(chan kafka.Event)
	err := producer.Produce(kafkaMessage, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Receive the delivery report or error from the delivery channels
	e := <-deliveryChan
	m := e.(*kafka.Message)

	// Check if there was an error during message delivery
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
	}

	// Close the delivery channel
	close(deliveryChan)

	return nil
}
