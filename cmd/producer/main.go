package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ShvetsovYura/pkafka_base/internal/producer"
)

func main() {
	// Проверяем, что количество параметров при запуске программы ровно 3
	if len(os.Args) != 3 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <topic>\n", os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера и имя топика
	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	p := producer.NewKafkaProducer(topic, bootstrapServers)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go p.Run(ctx, wg)
	wg.Wait()
}
