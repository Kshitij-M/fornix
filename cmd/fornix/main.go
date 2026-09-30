package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/omaveda/fornix/internal/config"
	"github.com/omaveda/fornix/internal/credentials"
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

	dependencies, err := buildServerDependencies(cfg)
	if err != nil {
		log.Fatalf("credential authority configuration: %v", err)
	}
	svc, err := server.NewWithDependencies(ctx, cfg, dependencies)
	if err != nil {
		log.Fatalf("server initialization: %v", err)
	}
	defer svc.Close()

	if err := svc.Run(ctx, cfg.Listen); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("server: %v", err)
	}
}

func buildServerDependencies(cfg config.Config) (server.ServerDependencies, error) {
	var dependencies server.ServerDependencies
	needsOpenAIManager := cfg.OpenAIEnabled && (cfg.Environment == "production" || strings.Contains(cfg.OpenAICredentialRef, "/"))
	if !cfg.FederationPollEnabled && !needsOpenAIManager {
		return dependencies, nil
	}
	if strings.TrimSpace(cfg.CredentialManagerURL) == "" {
		return dependencies, nil
	}
	tokenSource := credentials.TokenSource(nil)
	if strings.TrimSpace(cfg.CredentialManagerTokenRef) != "" {
		tokenSource = credentials.EnvTokenSource{Name: cfg.CredentialManagerTokenRef}
	}
	manager, err := credentials.NewHTTPSecretManager(credentials.HTTPSecretManagerConfig{
		Endpoint: cfg.CredentialManagerURL, Token: tokenSource,
		AllowPrivateNetworks: cfg.CredentialManagerAllowPrivate,
		Timeout:              cfg.CredentialManagerTimeout, Client: &http.Client{Timeout: cfg.CredentialManagerTimeout},
	})
	if err != nil {
		return server.ServerDependencies{}, err
	}
	if needsOpenAIManager {
		dependencies.OpenAISecretManager = manager
	}
	if cfg.FederationPollEnabled {
		dependencies.FederationSecretManager = manager
	}
	return dependencies, nil
}
