package broker

import "github.com/Shopify/sarama"

type Options struct {
	Addr         []string
	Kafka        *sarama.Config
	ErrorHandler Handler
}

type Option func(*Options)

type SubscribeOptions struct {
	AutoAck bool
	GroupID string
}

type SubscribeOption func(*SubscribeOptions)
