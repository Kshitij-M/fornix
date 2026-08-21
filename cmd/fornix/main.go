package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/omaveda/fornix/internal/config"
	"github.com/omaveda/fornix/internal/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		if err := runCLI(os.Args[1:]); err != nil {
			log.Print(err)
			os.Exit(2)
		}
		return
	}
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
