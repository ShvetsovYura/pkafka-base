package consumer

import "time"

type Options struct {
	Topics           []string
	ConsumerGroup    string
	BootstrapServers string
	PoolTimeout      time.Duration
	AutoOfsetReset   string
	EnableAutoCommit bool
}
