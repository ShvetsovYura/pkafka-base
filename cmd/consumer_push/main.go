package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ShvetsovYura/pkafka_base/internal/consumer"
)

func main() {

	if len(os.Args) < 3 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <group> <topics..>\n",
			os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера, группу и имя топиков
	bootstrapServers := os.Args[1]
	topics := os.Args[2:]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	c := consumer.NewKafkaConsumer(topics, "group2", bootstrapServers, 10, "earliest", true)
	c.Run(ctx)
}
