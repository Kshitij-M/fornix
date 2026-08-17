package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/omaveda/fornix/internal/config"
	"github.com/omaveda/fornix/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc, err := server.New(ctx, cfg)
	if err != nil {
		log.Fatalf("server initialization: %v", err)
	}
	defer svc.Close()

	if err := svc.Run(ctx, cfg.Listen); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("server: %v", err)
	}
}
