package main

import (
	"GolangIntegration/pkg/broker"
	"GolangIntegration/pkg/broker/kafka"
	"GolangIntegration/pkg/logger"
	"fmt"
)

func main() {
	kafka := kafka.NewBroker(&broker.Options{})
	csr, err := kafka.NewConsumer()
	fmt.Println(err)

	csr.Subscribe("test-1", func(evt broker.Event) error {
		fmt.Println(evt.Message().Header)
		fmt.Println(string(evt.Message().Body))
		logger.InfoF("Headers:%v, Body:%s", evt.Message().Header, string(evt.Message().Body))
		return nil
	})

	select {}
}
