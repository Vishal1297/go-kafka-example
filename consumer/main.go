package main

import (
	"fmt"
	"time"

	"github.com/Vishal1297/go-kafka-example/common"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": common.KafkaServers,
		"group.id":          common.GroupId,
		"auto.offset.reset": common.OffsetResetEarliest,
	})

	if err != nil {
		panic(err)
	}

	// c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)
	c.SubscribeTopics([]string{common.TopicName}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}