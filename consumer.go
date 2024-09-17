package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// Kafka broker URL
	kafkaBroker = "localhost:9092"

	// Kafka topic where data transformation messages are published
	kafkaTopic = "data-transform"

	// Consumer Group ID used by consumers to coordinate message consumption
	groupID = "data-transform-group"

	// The number of consumers
	numConsumers = 8

	// The name of the output CSV file for transformed data
	outputCSV = "output.csv"
)

var (
	// Global mutex for synchronizing access to stream writer
	mutex sync.Mutex
)

// Entry point of the application
func main() {
	// Parse the command-line arguments
	parseArgs()

	// Need to write the fields if the outputCSV file doesn't exist or is empty
	needToWriteFields := false
	if stat, err := os.Stat(outputCSV); os.IsNotExist(err) || stat.Size() == 0 {
		needToWriteFields = true
	}

	// Open the existing CSV file in append mode or create it if it doesn't exist
	file, err := os.OpenFile(outputCSV, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend|0664)
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	// CSV writer for writing data to a CSV file or stream
	writer := csv.NewWriter(file)
	defer writer.Flush()

	/// Write the header fields to the CSV file if needed
	if needToWriteFields {
		writeToCSV([][]string{{"modified", "publisher.name",
			"publisher.subOrganizationOf.name", "contactPoint.fn", "keyword"}}, writer)
	}

	// Create and run goroutines to consume messages from Kafka partitions concurrently
	var wg sync.WaitGroup
	for routineId := 0; routineId < numConsumers; routineId++ {
		wg.Add(1)
		go consumeMessages(routineId, &wg, writer)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Printf("\nTerminated gracefully\n")
}

// Parse command-line arguments
func parseArgs() {
	// Define a flag for the URL of the Kafka broker
	kafkaBroker_ := flag.String(
		"kafka-broker", kafkaBroker, "The URL of the Kafka broker")

	// Define a flag for the Kafka topic
	kafkaTopic_ := flag.String(
		"kafka-topic", kafkaTopic, "The Kafka topic")

	// Define a flag for the data transformation group ID
	groupID_ := flag.String(
		"group-id", groupID, "The consumer group ID")

	// Define a flag for the number of consumers
	numConsumers_ := flag.Int(
		"consumers", numConsumers, "The number of consumers")

	// Define a flag for the output CSV file name
	outputCSV_ := flag.String(
		"output", outputCSV, "The name of the output CSV file for transformed data")

	// Parse the command-line flags
	flag.Parse()

	// Assign the parsed flag values to the global variables
	kafkaBroker = *kafkaBroker_
	kafkaTopic = *kafkaTopic_
	groupID = *groupID_
	numConsumers = *numConsumers_
	outputCSV = *outputCSV_
}

// Consumes messages from a Kafka topic partition
func consumeMessages(routineId int, wg *sync.WaitGroup, writer *csv.Writer) {
	defer wg.Done()

	// User-specific properties that you must set
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker, // Kafka Broker
		"group.id":          groupID,     // Kafka Group ID
		"auto.offset.reset": "earliest"}) // Earliest offset

	// Failed to create consumer
	if err != nil {
		fmt.Printf("(%d): Failed to create consumer: %s\n", routineId, err)
		return
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic
	err = consumer.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		fmt.Printf("(%d): Failed to subscribe topic: %s\n", routineId, err)
		return
	}

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process the messages
	for {
		select {
		case <-sigchan:
			return
		default:
			// Read message with 100 ms timeout
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				// Write the consumed message
				writeMessage(e.Value, writer)
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			}
		}
	}
}

// Writes a raw message
func writeMessage(message []byte, writer *csv.Writer) {
	// Deserialize message into a map
	var data map[string][]string
	err := json.Unmarshal(message, &data)
	if err != nil {
		fmt.Printf("Error decoding JSON: %v\n", err)
		return
	}

	// Print message
	printMessage(&data)

	// Generate rows of data for CSV
	rows := combineArrays([][]string{
		data["modified"],
		data["publisher.name"],
		data["publisher.subOrganizationOf.name"],
		data["contactPoint.fn"],
		data["keyword"]})

	// Write rows to CSV file
	writeToCSV(rows, writer)
}

// Recursively generates combinations from multiple slices
func combineArrays(arrays [][]string) [][]string {
	var result [][]string
	var combination []string
	combineHelper(arrays, 0, combination, &result)
	return result
}

// Recursively builds combinations
func combineHelper(arrays [][]string, depth int, combination []string, result *[][]string) {
	if depth == len(arrays) {
		*result = append(*result, combination)
		return
	}

	for _, value := range arrays[depth] {
		// Create a new combination slice with the current value added.
		newCombination := append(combination, value)
		combineHelper(arrays, depth+1, newCombination, result)
	}
}

// Print a message
func printMessage(message *map[string][]string) {
	fmt.Printf("\nmodified: %s\n", (*message)["modified"])
	fmt.Printf("publisher.name: %s\n", (*message)["publisher.name"])
	fmt.Printf("publisher.subOrganizationOf.name: %s\n", (*message)["publisher.subOrganizationOf.name"])
	fmt.Printf("contactPoint.fn: %s\n", (*message)["contactPoint.fn"])
	fmt.Printf("keyword: %s\n", (*message)["keyword"])
}

// Writes newRows data to a CSV file using a global CSV writer (writer)
func writeToCSV(newRows [][]string, writer *csv.Writer) {
	// Lock the global mutex to ensure exclusive access to the writer
	mutex.Lock()
	defer mutex.Unlock()

	// Write each row from newRows to the CSV file
	for _, newRow := range newRows {
		writer.Write(newRow)
	}

	// Flush the CSV writer to ensure all buffered data is written to the file
	writer.Flush()

	// Check if there were any errors during the flush operation
	if err := writer.Error(); err != nil {
		fmt.Printf("Error flushing writer: %v\n", err)
	}
}
