package consumer

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/ShvetsovYura/pkafka_base/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Msg struct {
	Uuid  string `json:"uuid"`
	Value string `json:"value"`
}

type KConsumer struct {
	topics           []string
	group            string
	servers          string
	timeout          time.Duration
	consumer         *kafka.Consumer
	enableAutoCommit bool
}

const sessionTimeout = 6000
const fetchMinBytes = 1024 * 10

func NewKafkaConsumer(opts Options) *KConsumer {

	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  opts.BootstrapServers,
		"group.id":           opts.ConsumerGroup,
		"session.timeout.ms": sessionTimeout,
		"enable.auto.commit": opts.EnableAutoCommit,
		"auto.offset.reset":  opts.AutoOfsetReset,
		"fetch.min.bytes":    fetchMinBytes,
		// "max.poll.records":   1, // это не работатет https://github.com/confluentinc/confluent-kafka-go/issues/380)
	})

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
	}

	logger.Log.Info("Консьюмер создан", slog.Any("consumer", c))

	return &KConsumer{
		topics:           opts.Topics,
		group:            opts.ConsumerGroup,
		servers:          opts.BootstrapServers,
		timeout:          opts.PoolTimeout,
		enableAutoCommit: opts.EnableAutoCommit,
		consumer:         c,
	}
}

func (c *KConsumer) RunPush(ctx context.Context, bufferSize int) {
	var q = make(chan string, bufferSize)
	defer func() {
		c.consumer.Close()
		close(q)
	}()

	go messageWorker(ctx, q)
	err := c.consumer.SubscribeTopics(c.topics, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	run := true
	for run {
		select {
		case <-ctx.Done():
			logger.Log.Info("Передан сигнал завершения приложения приложение останавливается\n")
			run = false
		default:
			msg, err := c.consumer.ReadMessage(c.timeout)
			if err == nil {
				logger.Log.Info("message", slog.String("partition", msg.TopicPartition.String()), slog.String("value", string(msg.Value)))
				q <- msg.String()
			} else if !err.(kafka.Error).IsTimeout() {
				logger.Log.Error("Consumer error", slog.Any("err", err), slog.Any("msg", msg))
			}
		}
	}
}

func (c *KConsumer) RunPull(ctx context.Context) {
	defer c.consumer.Close()
	err := c.consumer.SubscribeTopics(c.topics, nil)
	var timer *time.Ticker = time.NewTicker(c.timeout)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	run := true
	for run {
		select {
		case <-ctx.Done():
			logger.Log.Info("Передан сигнал завершения приложения приложение останавливается")
			run = false
		case <-timer.C:
			msg, err := c.consumer.ReadMessage(c.timeout)
			if err == nil {
				logger.Log.Info("message", slog.String("partition", msg.TopicPartition.String()), slog.String("value", string(msg.Value)))
				_, err = c.consumer.CommitMessage(msg)
				if err != nil {
					logger.Log.Error("error on commit message", slog.Any("msg", msg))
				}
			} else if !err.(kafka.Error).IsTimeout() {
				logger.Log.Error("Consumer error", slog.Any("err", err), slog.Any("msg", msg))
			}
		}
	}
}

func messageWorker(ctx context.Context, jobs <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-jobs:
			logger.Log.Info("proccess messages", slog.Int("queue", len(jobs)))
			// time.Sleep(10 * time.Second)
		}
	}
}
