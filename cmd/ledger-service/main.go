// Command ledger-service starts the ledger HTTP server. No business logic here —
// only wiring, mirroring the cmd/ convention documented in BEST_PRACTICES.md.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"ledger-rail/ledger-service/internal/account"
	"ledger-rail/ledger-service/internal/httpapi"
	"ledger-rail/ledger-service/internal/kafka"
	"ledger-rail/ledger-service/internal/ledger"
	"ledger-rail/ledger-service/internal/outbox"
	"ledger-rail/ledger-service/internal/postgres"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"
	}
	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "transactions"
	}

	store, err := postgres.Open(dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer store.Close()

	producer := kafka.NewProducer(strings.Split(brokers, ","), topic)
	defer producer.Close()

	svc := ledger.NewService(store)
	accountSvc := account.NewService(store)
	server := httpapi.NewServer(svc, accountSvc)

	relay := outbox.NewRelay(store, producer, 200*time.Millisecond, 20)
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	go relay.Run(relayCtx)

	log.Printf("ledger-service listening on :%s", addr)
	if err := http.ListenAndServe(":"+addr, server); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
