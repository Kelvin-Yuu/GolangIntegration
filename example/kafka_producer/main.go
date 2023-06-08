package main

import (
	"GolangIntegration/pkg/broker"
	"GolangIntegration/pkg/broker/kafka"
	"fmt"
)

func main() {
	kafka := kafka.NewBroker(&broker.Options{})
	producer, error := kafka.NewProducer()
	error = producer.Publish("test-1", &broker.Message{
		Header: map[string]string{
			"标题": "Title1",
			"负载": "200",
		},
		Body: []byte("asdasd"),
	})
	error = kafka.Close()
	fmt.Println(error)

}
