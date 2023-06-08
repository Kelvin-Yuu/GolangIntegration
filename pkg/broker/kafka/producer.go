package kafka

import (
	"GolangIntegration/pkg/broker"
	"errors"
	"github.com/Shopify/sarama"
	"time"
)

type Producer struct {
	opts *broker.Options

	c sarama.Client
	p sarama.SyncProducer

	connected bool
}

func (pr *Producer) isConnected() bool {
	return pr.connected
}

func (pr *Producer) Connect() error {
	if pr.isConnected() {
		return nil
	}

	if pr.c != nil {
		pr.connected = true
		return nil
	}
	// kafka地址
	addr := pr.opts.Addr
	if len(addr) == 0 {
		addr = []string{"127.0.0.1:9092"}
	}

	config := pr.opts.Kafka
	if config == nil {
		config = sarama.NewConfig()
	}
	if !config.Version.IsAtLeast(sarama.V3_3_1_0) {
		config.Version = sarama.V3_3_1_0
	}
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 1 * time.Second
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.CompressionLevel = 9

	c, err := sarama.NewClient(addr, config)
	if err != nil {
		return err
	}

	p, err := sarama.NewSyncProducerFromClient(c)
	if err != nil {
		return err
	}

	pr.c = c
	pr.p = p
	pr.connected = true

	return nil
}

func (pr *Producer) Disconnect() error {
	if !pr.isConnected() {
		return nil
	}

	err := pr.p.Close()
	if err != nil {
		return err
	}

	if err := pr.c.Close(); err != nil {
		return err
	}
	pr.connected = false
	return nil
}

func (pr *Producer) Publish(topic string, msg *broker.Message) error {
	if !pr.isConnected() {
		return errors.New("[kafka] broker not connected")
	}

	var headers []sarama.RecordHeader
	for k, v := range msg.Header {
		var header sarama.RecordHeader
		header.Key = []byte(k)
		header.Value = []byte(v)
		headers = append(headers, header)
	}

	_, _, err := pr.p.SendMessage(&sarama.ProducerMessage{
		Topic:   topic,
		Headers: headers,
		Value:   sarama.ByteEncoder(msg.Body),
	})
	return err
}
