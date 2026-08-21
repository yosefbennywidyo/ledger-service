// Command ledger-service starts the ledger HTTP server. No business logic here —
// only wiring, mirroring the cmd/ convention documented in BEST_PRACTICES.md.
package main

import (
	"log"
	"net/http"
	"os"

	"ledger-rail/ledger-service/internal/httpapi"
	"ledger-rail/ledger-service/internal/ledger"
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

	store, err := postgres.Open(dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer store.Close()

	svc := ledger.NewService(store)
	server := httpapi.NewServer(svc)

	log.Printf("ledger-service listening on :%s", addr)
	if err := http.ListenAndServe(":"+addr, server); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
