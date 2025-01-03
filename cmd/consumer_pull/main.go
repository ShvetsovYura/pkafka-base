package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ShvetsovYura/pkafka_base/internal/consumer"
	"github.com/ShvetsovYura/pkafka_base/internal/logger"
)

func main() {
	logger.Init()
	if len(os.Args) < 3 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <group> <topics..>\n",
			os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера, группу и имя топиков
	bootstrapServers := os.Args[1]
	consumerGroup := os.Args[2]
	topics := os.Args[2:]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	opts := consumer.Options{
		Topics:           topics,
		BootstrapServers: bootstrapServers,
		ConsumerGroup:    consumerGroup,
		PoolTimeout:      5000,
		AutoOfsetReset:   "earliest",
		EnableAutoCommit: false,
	}
	c := consumer.NewKafkaConsumer(opts)
	c.RunPull(ctx)
}
