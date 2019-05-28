package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

// TimestampLayout defines the format to parse timestamps into the time.Time tyep
const TimestampLayout = "2006-01-02 15:04:05.000000"

// ErrorLevel is the string value for errors as determined by a log's "level" field
const ErrorLevel = "ERROR"

// Timestamp is used to parse JSON "timestamp" input into the time.Time type
// Adapted from https://ustrajunior.com/blog/json-unmarshal-custom-date-formats/
type Timestamp struct {
	time.Time
}

// UnmarshalJSON defines the interface for unmarshalling the "timestamp" field into a time.Time type
func (t *Timestamp) UnmarshalJSON(input []byte) error {
	strInput := string(input)
	strInput = strings.Trim(strInput, `"`)
	newTime, err := time.Parse(TimestampLayout, strInput)
	if err != nil {
		return err
	}

	t.Time = newTime
	return nil
}

// Log represents a single JSON-encoded log event
type Log struct {
	Service       string    `json:"service"`
	Level         string    `json:"level"`
	Timestamp     Timestamp `json:"timestamp"`
	Operation     string    `json:"operation"`
	Message       string    `json:"message"`
	TransactionID string    `json:"transaction_id"`
}

// IsError determines if a Log is an error according to its level
func (log *Log) IsError() bool {
	return log.Level == ErrorLevel
}

// Logs is a list of logs represented as a Go slice
type Logs []Log

// Interface to sort Logs by timestamp
// Based on: https://stackoverflow.com/questions/23121026/sorting-by-time-time-in-golang
func (logs Logs) Len() int {
	return len(logs)
}

// Define compare (by time)
func (logs Logs) Less(i, j int) bool {
	return logs[i].Timestamp.Time.Before(logs[j].Timestamp.Time)
}

// Define swap over an array
func (logs Logs) Swap(i, j int) {
	logs[i], logs[j] = logs[j], logs[i]
}

// LongestTransaction returns a formatted string containing
// the transaction with the longest duration, as determined by the first
// and last timestamp within the Logs associated with a transaction
func (logs *Logs) LongestTransaction() string {
	var longestDuration time.Duration
	longestTransaction := ""
	transactions := map[string]Logs{}
	// Create a map of Logs indexed by the log.TransactionID field
	for _, log := range *logs {
		transactions[log.TransactionID] = append(transactions[log.TransactionID], log)
	}
	for id, list := range transactions {
		// Sort Logs by Timestamp
		sort.Sort(list)
		firstTime := list[0]
		lastTime := list[len(list)-1]
		// Get the duration between the first and last timestamp in transaction
		// https://stackoverflow.com/questions/40260599/difference-between-two-time-time-objects/40260666
		duration := lastTime.Timestamp.Sub(firstTime.Timestamp.Time)
		if duration > longestDuration {
			// Set longest duration if longer than duration seen so far
			longestTransaction = id
			longestDuration = duration
		}
	}
	return fmt.Sprintf("%s (%s)", longestTransaction, longestDuration)
}

// OperationWithMostErrors returns a formatted string containing
// the operation with the most errors (and its error count)
func (logs *Logs) OperationWithMostErrors() string {
	mostErrors := 0
	var operationWithMostErrors string
	// Create a map of Logs indexed by the log.Operation field
	operations := map[string]Logs{}
	for _, log := range *logs {
		operations[log.Operation] = append(operations[log.Operation], log)
	}
	// Count the number of errors for each operation, and set it to max
	// if greater than most errors seen thus far
	for operation, list := range operations {
		numErrors := 0
		for _, log := range list {
			if log.IsError() {
				numErrors++
			}
		}
		if numErrors > mostErrors {
			operationWithMostErrors = operation
			mostErrors = numErrors
		}
	}
	return fmt.Sprintf("%s (%d Errors)", operationWithMostErrors, mostErrors)
}

func main() {
	args := os.Args[1:]
	fileName := args[0]
	// Read filename given by first argument
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
	}
	// Parse JSON file and analyze logs
	logs := Logs{}
	err = json.Unmarshal(data, &logs)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Total Log Entries:", len(logs))
	fmt.Println("Longest Transaction:", logs.LongestTransaction())
	fmt.Println("Operation with Most Errors:", logs.OperationWithMostErrors())
}
