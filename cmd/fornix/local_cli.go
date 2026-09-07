package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/credentials"
	"github.com/omaveda/fornix/internal/profile"
	localruntime "github.com/omaveda/fornix/internal/runtime"
	"github.com/omaveda/fornix/internal/version"
)

const (
	localDefaultWorkspace = "local"
	localDefaultServerURL = "http://127.0.0.1:8201"
	localAPIKeyRef        = "local-api"
	localDatabaseRef      = "database-password"
	localBootstrapRef     = "bootstrap-key"
)

type localOptions struct {
	command       string
	home          string
	serverURL     string
	port          int
	workspace     string
	key           string
	bootstrapKey  string
	repository    string
	provider      string
	model         string
	prompt        string
	check         string
	service       string
	output        string
	json          bool
	follow        bool
	yes           bool
	purgeData     bool
	detach        bool
	pull          bool
	keepData      bool
	maxCost       float64
	maxTime       time.Duration
	maxTurns      int
	maxOutput     int
	maxContextB   int
	maxContextTok int
}

type localSession struct {
	store       *profile.Store
	profile     profile.Metadata
	credentials *credentials.Store
}

func runLocalCLI(args []string) error {
	opts, err := parseLocalOptions(args)
	if err != nil {
		return err
	}
	ctx := context.Background()
	switch opts.command {
	case "start", "setup":
		return runLocalStart(ctx, opts)
	case "stop":
		return runLocalLifecycle(ctx, opts, "stop")
	case "restart":
		return runLocalLifecycle(ctx, opts, "restart")
	case "status":
		return runLocalStatus(ctx, opts)
	case "logs":
		return runLocalLogs(ctx, opts)
	case "doctor":
		return runLocalDoctor(ctx, opts)
	case "demo":
		return runLocalDemo(ctx, opts)
	case "run":
		return runLocalPrompt(ctx, opts)
	case "upgrade":
		return runLocalUpgrade(ctx, opts)
	case "uninstall":
		return runLocalUninstall(ctx, opts)
	case "support":
		return runLocalSupportBundle(ctx, opts)
	default:
		return fmt.Errorf("unknown local command %q", opts.command)
	}
}

func parseLocalOptions(args []string) (localOptions, error) {
	if len(args) == 0 {
		return localOptions{}, errors.New("a command is required; run 'fornix --help'")
	}
	opts := localOptions{command: args[0], json: false, maxCost: 1, maxTime: 30 * time.Second, maxTurns: 3, maxOutput: 512, maxContextB: 32768, maxContextTok: 2048}
	var prompt []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			prompt = append(prompt, args[i+1:]...)
			break
		}
		if arg == "--json" {
			opts.json = true
			continue
		}
		if arg == "--no-json" {
			opts.json = false
			continue
		}
		if arg == "--follow" {
			opts.follow = true
			continue
		}
		if arg == "--yes" {
			opts.yes = true
			continue
		}
		if arg == "--purge-data" {
			opts.purgeData = true
			continue
		}
		if arg == "--detach" {
			opts.detach = true
			continue
		}
		if arg == "--pull" {
			opts.pull = true
			continue
		}
		if arg == "--keep-data" {
			opts.keepData = true
			continue
		}
		name, value, hasValue := localFlag(arg)
		if !hasValue {
			if strings.HasPrefix(arg, "-") {
				return localOptions{}, fmt.Errorf("%s requires a value", arg)
			}
			if opts.command == "run" || opts.command == "demo" {
				prompt = append(prompt, arg)
				continue
			}
			return localOptions{}, fmt.Errorf("unexpected argument %q", arg)
		}
		if value == "" && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			i++
			value = args[i]
		}
		switch name {
		case "home":
			opts.home = value
		case "url":
			opts.serverURL = value
		case "port":
			parsed, parseErr := positiveLocalInt(value, "--port")
			if parseErr != nil || parsed < 1024 || parsed > 65535 {
				return localOptions{}, fmt.Errorf("--port must be between 1024 and 65535")
			}
			opts.port = parsed
		case "workspace":
			opts.workspace = value
		case "key":
			opts.key = value
		case "bootstrap-key":
			opts.bootstrapKey = value
		case "repo":
			opts.repository = value
		case "provider":
			opts.provider = strings.ToLower(strings.TrimSpace(value))
		case "model":
			opts.model = value
		case "check":
			opts.check = strings.ToLower(strings.TrimSpace(value))
		case "service":
			opts.service = value
		case "max-cost":
			n, parseErr := strconv.ParseFloat(value, 64)
			if parseErr != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
				return localOptions{}, fmt.Errorf("--max-cost must be a non-negative number")
			}
			opts.maxCost = n
		case "max-time":
			duration, parseErr := time.ParseDuration(value)
			if parseErr != nil || duration <= 0 {
				return localOptions{}, fmt.Errorf("--max-time must be a positive duration")
			}
			opts.maxTime = duration
		case "max-turns":
			parsed, parseErr := positiveLocalInt(value, "--max-turns")
			if parseErr != nil {
				return localOptions{}, parseErr
			}
			opts.maxTurns = parsed
		case "max-output-tokens":
			parsed, parseErr := positiveLocalInt(value, "--max-output-tokens")
			if parseErr != nil {
				return localOptions{}, parseErr
			}
			opts.maxOutput = parsed
		case "max-context-bytes":
			parsed, parseErr := positiveLocalInt(value, "--max-context-bytes")
			if parseErr != nil {
				return localOptions{}, parseErr
			}
			opts.maxContextB = parsed
		case "max-context-tokens":
			parsed, parseErr := positiveLocalInt(value, "--max-context-tokens")
			if parseErr != nil {
				return localOptions{}, parseErr
			}
			opts.maxContextTok = parsed
		case "output":
			opts.output = value
		default:
			return localOptions{}, fmt.Errorf("unknown option --%s", name)
		}
	}
	opts.prompt = strings.TrimSpace(strings.Join(prompt, " "))
	if opts.command == "run" && opts.prompt == "" {
		return localOptions{}, errors.New("run requires a prompt, for example: fornix run --repo . \"Explain this repository\"")
	}
	if opts.provider == "" {
		opts.provider = "fake"
	}
	if opts.provider != "fake" && opts.provider != "openai" && opts.provider != "ollama" {
		return localOptions{}, fmt.Errorf("unsupported provider %q; choose fake, openai, or ollama", opts.provider)
	}
	if opts.model == "" {
		opts.model = localDefaultModel(opts.provider)
	}
	return opts, nil
}

// localDefaultModel keeps the provider reference complete before it reaches
// the durable model-call ledger. The ledger rejects an empty model name, so an
// omitted CLI flag must resolve to a stable configured default at the CLI
// boundary rather than relying on a provider to repair it after reservation.
func localDefaultModel(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "openai":
		return envOr("FORNIX_OPENAI_MODEL", "gpt-4o-mini")
	case "ollama":
		return envOr("FORNIX_OLLAMA_MODEL", "nomic-embed-text")
	default:
		return "fake-model"
	}
}

func localFlag(arg string) (string, string, bool) {
	if !strings.HasPrefix(arg, "--") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(arg, "--")
	if index := strings.IndexByte(trimmed, '='); index >= 0 {
		return trimmed[:index], trimmed[index+1:], true
	}
	return trimmed, "", true
}

func positiveLocalInt(value, name string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func localProfileRoot(explicit string) (string, error) {
	root := strings.TrimSpace(explicit)
	if root == "" {
		root = strings.TrimSpace(os.Getenv("FORNIX_HOME"))
	}
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve user home: %w", err)
		}
		root = filepath.Join(home, ".fornix")
	}
	return profile.ValidateRoot(root)
}

func localRuntimeName(project string) string {
	project = strings.ToLower(strings.TrimSpace(project))
	project = strings.TrimPrefix(project, "fornix-")
	if project == "" {
		return "local"
	}
	if _, err := localruntime.NewProfile(filepath.Join(string(filepath.Separator), "fornix-profile"), project); err != nil {
		return "local"
	}
	return project
}

func localRuntimeEnvironment(databasePassword, bootstrapKey []byte, openAIEnabled bool) []string {
	values := map[string]string{
		"FORNIX_DATABASE_PASSWORD": string(databasePassword),
		"FORNIX_BOOTSTRAP_KEY":     string(bootstrapKey),
		"FORNIX_WORKER_ENABLED":    strconv.FormatBool(!parseLocalBool("FORNIX_WORKER_DISABLED")),
		"FORNIX_OPENAI_ENABLED":    strconv.FormatBool(openAIEnabled),
		"FORNIX_OPENAI_API_KEY":    "",
		"FORNIX_OPENAI_BASE_URL":   envOr("FORNIX_OPENAI_BASE_URL", "https://api.openai.com/v1"),
		"FORNIX_OPENAI_MODEL":      envOr("FORNIX_OPENAI_MODEL", "gpt-4o-mini"),
	}
	if openAIEnabled {
		values["FORNIX_OPENAI_API_KEY"] = os.Getenv("FORNIX_OPENAI_API_KEY")
	}
	environment := append([]string(nil), os.Environ()...)
	for key, value := range values {
		prefix := key + "="
		replaced := false
		for index, entry := range environment {
			if strings.HasPrefix(entry, prefix) {
				environment[index] = prefix + value
				replaced = true
				break
			}
		}
		if !replaced {
			environment = append(environment, prefix+value)
		}
	}
	return environment
}

func openLocalSession(opts localOptions, create bool) (*localSession, error) {
	root, err := localProfileRoot(opts.home)
	if err != nil {
		return nil, err
	}
	store, err := profile.New(root)
	if err != nil {
		return nil, err
	}
	p, err := store.Load()
	if err != nil {
		if !errors.Is(err, profile.ErrNotFound) || !create {
			if errors.Is(err, profile.ErrNotFound) {
				return nil, errors.New("Fornix is not set up; run 'fornix start' first")
			}
			return nil, err
		}
		workspace := opts.workspace
		if workspace == "" {
			workspace = envOr("FORNIX_WORKSPACE_ID", localDefaultWorkspace)
		}
		serverURL := opts.serverURL
		if serverURL == "" {
			serverURL = envOr("FORNIX_URL", localDefaultServerURL)
		}
		project := strings.TrimSpace(os.Getenv("FORNIX_RUNTIME_PROJECT"))
		if project == "" {
			project = "fornix-local"
		}
		if _, projectErr := localruntime.NewProfile(filepath.Join(string(filepath.Separator), "fornix-profile"), localRuntimeName(project)); projectErr != nil {
			return nil, fmt.Errorf("FORNIX_RUNTIME_PROJECT is invalid: %w", projectErr)
		}
		p = profile.NewMetadata("local")
		p.ServerURL = strings.TrimRight(serverURL, "/")
		p.Port = opts.port
		p.WorkspaceID = workspace
		p.ActorID = "local-operator"
		p.CredentialRef = "local/api"
		p.RuntimeProject = project
		p.RuntimeVersion = version.Current().Version
		if err := saveLocalSecrets(store, opts, true); err != nil {
			return nil, err
		}
		if err := store.Save(p); err != nil {
			return nil, err
		}
	}
	credentialStore, err := credentials.New(root)
	if err != nil {
		return nil, err
	}
	if ref, refErr := credentials.ParseRef(localDatabaseRef); refErr != nil {
		return nil, refErr
	} else if _, readErr := credentialStore.Read(ref); readErr != nil {
		return nil, fmt.Errorf("local database credential is unavailable: %w", readErr)
	}
	if ref, refErr := credentials.ParseRef(localBootstrapRef); refErr != nil {
		return nil, refErr
	} else if _, readErr := credentialStore.Read(ref); readErr != nil {
		return nil, fmt.Errorf("local bootstrap credential is unavailable: %w", readErr)
	}
	return &localSession{store: store, profile: p, credentials: credentialStore}, nil
}

func saveLocalSecrets(store *profile.Store, opts localOptions, create bool) error {
	root := store.Root()
	credentialStore, err := credentials.New(root)
	if err != nil {
		return err
	}
	databaseRef, err := credentials.ParseRef(localDatabaseRef)
	if err != nil {
		return err
	}
	if _, readErr := credentialStore.Read(databaseRef); errors.Is(readErr, credentials.ErrNotFound) && create {
		secret := strings.TrimSpace(os.Getenv("FORNIX_DB_PASSWORD"))
		if secret == "" {
			secret, err = randomLocalSecret("fornix_db_")
			if err != nil {
				return err
			}
		}
		value, secretErr := credentials.NewSecret([]byte(secret))
		if secretErr != nil {
			return secretErr
		}
		defer value.Clear()
		if err := credentialStore.Write(databaseRef, value); err != nil {
			return err
		}
	} else if readErr != nil {
		return fmt.Errorf("read local database credential: %w", readErr)
	}
	bootstrapRef, err := credentials.ParseRef(localBootstrapRef)
	if err != nil {
		return err
	}
	if _, readErr := credentialStore.Read(bootstrapRef); errors.Is(readErr, credentials.ErrNotFound) && create {
		secret := strings.TrimSpace(opts.bootstrapKey)
		if secret == "" {
			secret = strings.TrimSpace(os.Getenv("FORNIX_BOOTSTRAP_KEY"))
		}
		if secret == "" {
			secret, err = randomLocalSecret("fornix_bootstrap_")
			if err != nil {
				return err
			}
		}
		value, secretErr := credentials.NewSecret([]byte(secret))
		if secretErr != nil {
			return secretErr
		}
		defer value.Clear()
		if err := credentialStore.Write(bootstrapRef, value); err != nil {
			return err
		}
	} else if readErr != nil {
		return fmt.Errorf("read local bootstrap credential: %w", readErr)
	}
	return nil
}

func (s *localSession) manager(opts localOptions) (*localruntime.Manager, error) {
	databaseRef, err := credentials.ParseRef(localDatabaseRef)
	if err != nil {
		return nil, err
	}
	databasePassword, err := s.credentials.Read(databaseRef)
	if err != nil {
		return nil, err
	}
	defer databasePassword.Clear()
	bootstrapRef, err := credentials.ParseRef(localBootstrapRef)
	if err != nil {
		return nil, err
	}
	bootstrapKey, err := s.credentials.Read(bootstrapRef)
	if err != nil {
		return nil, err
	}
	defer bootstrapKey.Clear()
	port, err := resolvedLocalPort(opts, s.profile)
	if err != nil {
		return nil, err
	}
	openAIEnabled := parseLocalBool("FORNIX_OPENAI_ENABLED") || opts.provider == "openai"
	if openAIEnabled && strings.TrimSpace(os.Getenv("FORNIX_OPENAI_API_KEY")) == "" {
		return nil, errors.New("OpenAI is explicitly enabled but FORNIX_OPENAI_API_KEY is not set")
	}
	runtimeVersion := s.profile.RuntimeVersion
	if runtimeVersion == "" {
		runtimeVersion = version.Current().Version
	}
	runtimeProfile, err := localruntime.NewProfile(s.store.Root(), localRuntimeName(s.profile.RuntimeProject))
	if err != nil {
		return nil, err
	}
	manifest, err := localruntime.DefaultManifestConfig(runtimeVersion)
	if err != nil {
		return nil, err
	}
	if image := strings.TrimSpace(os.Getenv("FORNIX_IMAGE")); image != "" {
		manifest.FornixImage = image
	}
	if image := strings.TrimSpace(os.Getenv("FORNIX_DB_IMAGE")); image != "" {
		manifest.PostgresImage = image
	}
	manifest.AppPort = uint16(port)
	repository := opts.repository
	if repository == "" {
		repository = s.profile.RepositoryMount
	}
	if repository != "" {
		resolved, resolveErr := resolveLocalRepository(repository)
		if resolveErr != nil {
			return nil, resolveErr
		}
		manifest.RepositoryPath = resolved
	}
	dockerPath, err := localDockerExecutable()
	if err != nil {
		return nil, err
	}
	environment := localRuntimeEnvironment(databasePassword.Bytes(), bootstrapKey.Bytes(), openAIEnabled)
	return localruntime.NewManagerWithEnvironment(runtimeProfile, manifest, localruntime.OSExecutor{}, dockerPath, localruntime.DefaultLimits(), environment)
}

// resolvedLocalPort applies the explicit CLI, environment, persisted profile,
// and default precedence once, so every lifecycle command addresses the same
// managed endpoint after a custom-port first run.
func resolvedLocalPort(opts localOptions, metadata profile.Metadata) (int, error) {
	port := metadata.Port
	if raw := strings.TrimSpace(os.Getenv("FORNIX_PORT")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("FORNIX_PORT must be an integer")
		}
		port = parsed
	}
	if opts.port > 0 {
		port = opts.port
	}
	if port == 0 {
		port = 8201
	}
	if port < 1024 || port > 65535 {
		return 0, errors.New("local port must be between 1024 and 65535")
	}
	return port, nil
}

// localDockerExecutable resolves the host runtime once at the CLI boundary.
// A few macOS desktop environments launch GUI processes with a reduced PATH,
// so the explicit override and common absolute locations keep the managed
// runtime reliable without asking users to understand Compose internals.
func localDockerExecutable() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("FORNIX_DOCKER_PATH")); configured != "" {
		if !filepath.IsAbs(configured) {
			return "", errors.New("FORNIX_DOCKER_PATH must be an absolute executable path")
		}
		if info, err := os.Stat(configured); err != nil || info.IsDir() {
			return "", errors.New("FORNIX_DOCKER_PATH does not point to an executable")
		}
		return configured, nil
	}
	if resolved, err := exec.LookPath("docker"); err == nil {
		return resolved, nil
	}
	for _, candidate := range []string{"/usr/local/bin/docker", "/opt/homebrew/bin/docker", "/usr/bin/docker"} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("Docker executable was not found; install Docker Desktop on macOS or Docker Engine plus Compose on Linux")
}

func (s *localSession) operator(opts localOptions) (*operatorCLI, error) {
	key := strings.TrimSpace(opts.key)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("FORNIX_KEY"))
	}
	if key == "" {
		if ref, refErr := credentials.ParseRef(s.profile.CredentialRef); refErr == nil {
			if secret, readErr := s.credentials.Read(ref); readErr == nil {
				key = string(secret.Bytes())
				secret.Clear()
			}
		}
	}
	bootstrapKey := strings.TrimSpace(opts.bootstrapKey)
	if bootstrapKey == "" {
		bootstrapKey = strings.TrimSpace(os.Getenv("FORNIX_BOOTSTRAP_KEY"))
	}
	if bootstrapKey == "" {
		if ref, refErr := credentials.ParseRef(localBootstrapRef); refErr == nil {
			if secret, readErr := s.credentials.Read(ref); readErr == nil {
				bootstrapKey = string(secret.Bytes())
				secret.Clear()
			}
		}
	}
	serverURL := s.profile.ServerURL
	if opts.serverURL != "" {
		serverURL = strings.TrimRight(opts.serverURL, "/")
	}
	return &operatorCLI{baseURL: strings.TrimRight(serverURL, "/"), key: key, bootstrapKey: bootstrapKey, workspace: s.profile.WorkspaceID, jsonOutput: true, client: &http.Client{Timeout: 10 * time.Minute}}, nil
}

func runLocalStart(ctx context.Context, opts localOptions) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	session, err := openLocalSession(opts, true)
	if err != nil {
		return err
	}
	repository := opts.repository
	if repository == "" {
		repository = session.profile.RepositoryMount
	}
	opts.repository = repository
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	port, err := resolvedLocalPort(opts, session.profile)
	if err != nil {
		return err
	}
	if _, err := manager.CheckDocker(ctx); err != nil {
		return fmt.Errorf("Docker is required for the managed local runtime; install Docker Desktop on macOS or Docker Engine plus Compose on Linux: %w", err)
	}
	if opts.pull {
		if _, err := manager.Pull(ctx); err != nil {
			return fmt.Errorf("pull local runtime images: %w", err)
		}
	}
	if _, err := manager.Start(ctx); err != nil {
		return fmt.Errorf("start local runtime: %w", err)
	}
	session.profile.ServerURL = manager.ServerURL()
	session.profile.Port = port
	if err := waitLocalReady(ctx, manager.ServerURL()); err != nil {
		return err
	}
	cli, err := session.operator(opts)
	if err != nil {
		return err
	}
	toolRoot := ""
	if repository != "" {
		toolRoot = "/workspace/repository"
	}
	if err := session.ensureWorkspace(cli, toolRoot); err != nil {
		return err
	}
	if repository != "" {
		resolved, resolveErr := resolveLocalRepository(repository)
		if resolveErr != nil {
			return resolveErr
		}
		session.profile.RepositoryMount = resolved
	}
	if err := session.store.Save(session.profile); err != nil {
		return err
	}
	return printLocalStart(session, manager, opts)
}

func (s *localSession) ensureWorkspace(cli *operatorCLI, toolRoot string) error {
	if cli.bootstrapKey == "" && cli.key == "" {
		return errors.New("local credentials are unavailable; run 'fornix start' again")
	}
	response, err := cli.request(http.MethodPost, "/v1/operator/workspaces/bootstrap", map[string]any{
		"workspace_id":     s.profile.WorkspaceID,
		"display_name":     "Fornix Local Workspace",
		"subject":          "local-operator",
		"default_provider": "fake",
		"tool_root":        toolRoot,
		"idempotency_key":  "local-bootstrap:" + s.profile.WorkspaceID,
	}, true)
	if err != nil {
		return fmt.Errorf("bootstrap local workspace: %w", err)
	}
	if token := nestedString(response, "api_key_token"); token != "" {
		ref, refErr := credentials.ParseRef(s.profile.CredentialRef)
		if refErr != nil {
			return refErr
		}
		secret, secretErr := credentials.NewSecret([]byte(token))
		if secretErr != nil {
			return secretErr
		}
		defer secret.Clear()
		if err := s.credentials.Write(ref, secret); err != nil {
			return err
		}
		cli.key = token
	}
	if cli.key == "" {
		return errors.New("workspace bootstrap did not return or preserve a local API credential")
	}
	if identityID := nestedID(response, "identity", "id"); identityID != "" {
		s.profile.ActorID = identityID
	}
	return nil
}

func printLocalStart(session *localSession, manager *localruntime.Manager, opts localOptions) error {
	provider := opts.provider
	if provider == "" {
		provider = "fake"
	}
	result := map[string]any{"ready": true, "server_url": manager.ServerURL(), "workspace_id": session.profile.WorkspaceID, "actor_id": session.profile.ActorID, "provider": provider, "runtime_project": session.profile.RuntimeProject, "repository_mount": session.profile.RepositoryMount, "version": version.Current()}
	if opts.json {
		return printLocalJSON(result)
	}
	fmt.Fprintln(os.Stdout, "Fornix is ready.")
	fmt.Fprintf(os.Stdout, "Workspace: %s\nRuntime:   Docker\nProvider:  %s\nAddress:   %s\n\nTry:\n  fornix run --repo . \"Explain this repository\"\n", session.profile.WorkspaceID, provider, manager.ServerURL())
	return nil
}

func runLocalLifecycle(ctx context.Context, opts localOptions, operation string) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	port, err := resolvedLocalPort(opts, session.profile)
	if err != nil {
		return err
	}
	var result localruntime.Result
	switch operation {
	case "stop":
		result, err = manager.Stop(ctx)
	case "restart":
		result, err = manager.Restart(ctx)
	}
	if err != nil {
		return err
	}
	session.profile.Port = port
	session.profile.ServerURL = manager.ServerURL()
	if err := session.store.Save(session.profile); err != nil {
		return err
	}
	return printLocalCommandResult(operation, result, opts.json)
}

func runLocalStatus(ctx context.Context, opts localOptions) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	result, err := manager.Status(ctx)
	if err != nil {
		return err
	}
	if opts.json {
		services, parseErr := localruntime.ParseStatus(result.Stdout)
		if parseErr == nil {
			return printLocalJSON(map[string]any{"workspace_id": session.profile.WorkspaceID, "server_url": manager.ServerURL(), "services": services})
		}
		return printLocalJSON(map[string]any{"workspace_id": session.profile.WorkspaceID, "raw": result.Stdout})
	}
	fmt.Fprintf(os.Stdout, "Workspace: %s\nAddress:   %s\n%s", session.profile.WorkspaceID, manager.ServerURL(), result.Stdout)
	return nil
}

func runLocalLogs(ctx context.Context, opts localOptions) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	result, err := manager.Logs(ctx, localruntime.LogsOptions{Service: opts.service, Tail: 200, Follow: opts.follow})
	if err != nil {
		return err
	}
	if result.Stdout != "" {
		_, _ = io.WriteString(os.Stdout, result.Stdout)
	}
	return nil
}

func runLocalDoctor(ctx context.Context, opts localOptions) error {
	report := map[string]any{"version": version.Current(), "checks": map[string]any{}}
	checks := report["checks"].(map[string]any)
	root, err := localProfileRoot(opts.home)
	if err != nil {
		checks["profile"] = map[string]any{"status": "fail", "message": err.Error()}
		return printDoctorReport(report, opts.json)
	}
	store, err := profile.New(root)
	if err != nil {
		checks["profile"] = map[string]any{"status": "fail", "message": err.Error()}
		return printDoctorReport(report, opts.json)
	}
	session, loadErr := openLocalSession(opts, false)
	if loadErr != nil {
		checks["profile"] = map[string]any{"status": "warning", "message": "not initialized; run 'fornix start'"}
	} else {
		checks["profile"] = map[string]any{"status": "pass", "workspace_id": session.profile.WorkspaceID, "runtime_project": session.profile.RuntimeProject}
		manager, managerErr := session.manager(opts)
		if managerErr != nil {
			checks["configuration"] = map[string]any{"status": "fail", "message": managerErr.Error()}
		} else if _, dockerErr := manager.CheckDocker(ctx); dockerErr != nil {
			checks["docker"] = map[string]any{"status": "fail", "message": "Docker daemon or Compose is unavailable"}
		} else {
			checks["docker"] = map[string]any{"status": "pass"}
		}
	}
	if opts.check == "provider" || opts.check == "all" || opts.check == "" {
		configured := strings.TrimSpace(os.Getenv("FORNIX_OPENAI_API_KEY")) != ""
		checks["provider"] = map[string]any{"status": "pass", "openai_configured": configured, "remote_call": "not checked"}
	}
	if opts.check == "repository" && opts.repository != "" {
		if resolved, resolveErr := resolveLocalRepository(opts.repository); resolveErr != nil {
			checks["repository"] = map[string]any{"status": "fail", "message": resolveErr.Error()}
		} else {
			checks["repository"] = map[string]any{"status": "pass", "path": resolved}
		}
	}
	_ = store
	return printDoctorReport(report, opts.json)
}

func runLocalDemo(ctx context.Context, opts localOptions) error {
	if opts.provider != "fake" {
		return errors.New("demo is offline and only supports the deterministic fake provider")
	}
	if err := runLocalStart(ctx, opts); err != nil {
		return err
	}
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	cli, err := session.operator(opts)
	if err != nil {
		return err
	}
	return runLocalDemoWorkflow(cli, session, opts)
}

func runLocalDemoWorkflow(cli *operatorCLI, session *localSession, opts localOptions) error {
	prefix := "local-demo:" + session.profile.WorkspaceID
	runResponse, err := cli.request(http.MethodPost, "/v1/agent/run", map[string]any{
		"workspace_id": session.profile.WorkspaceID, "idempotency_key": prefix + ":run", "goal": "Run the deterministic Fornix local demonstration and report its bounded result.",
		"provider": map[string]any{"provider": "fake", "model": "fake-model"}, "budget": map[string]any{"max_turns": 2, "max_model_steps": 2, "max_tool_calls": 1, "max_context_bytes": 8192, "max_output_tokens": 256, "max_wall_time_ms": 15000, "max_cost_usd": 1, "max_tool_attempts": 1},
	}, false)
	if err != nil {
		return err
	}
	runID := nestedID(runResponse, "run", "id")
	if runID == "" {
		return errors.New("demo did not return an agent run id")
	}
	run, ok := runResponse["run"].(map[string]any)
	if !ok {
		return errors.New("demo response did not contain a run")
	}
	replay, err := cli.request(http.MethodPost, "/v1/agent/run/"+url.PathEscape(runID)+"/replay?workspace_id="+url.QueryEscape(session.profile.WorkspaceID), map[string]any{}, false)
	if err != nil {
		return err
	}
	report, _ := json.Marshal(map[string]any{"workflow": "fornix-local-demo", "run_id": runID, "state_hash": run["state_hash"], "context_hash": run["context_hash"], "replay": replay})
	artifact, err := cli.request(http.MethodPost, "/v1/artifacts", map[string]any{"workspace_id": session.profile.WorkspaceID, "kind": "fornix-report", "media_type": "application/json", "raw": report, "source_kind": "agent_run", "source_id": runID, "role": "report", "idempotency_key": prefix + ":artifact"}, false)
	if err != nil {
		return err
	}
	evidence, err := cli.request(http.MethodPost, "/v1/evidence", map[string]any{"workspace_id": session.profile.WorkspaceID, "source_reference": "agent-run:" + runID + ":report", "deduplication_key": prefix + ":evidence", "kind": "agent-report", "media_type": "application/json", "gist": "deterministic local demonstration report", "detail": "artifact-backed report for the local demonstration", "raw_payload": json.RawMessage(report)}, false)
	if err != nil {
		return err
	}
	receipt, err := cli.request(http.MethodPost, "/v1/work-receipts", map[string]any{
		"workspace_id": session.profile.WorkspaceID, "idempotency_key": prefix + ":receipt", "work_kind": "agent_run", "work_id": runID, "replay_hash": stringValue(run, "state_hash"),
		"steps":     []any{map[string]any{"ordinal": 0, "id": "agent-run", "name": "deterministic demo run", "kind": "agent", "status": "succeeded", "source_kind": "agent_run", "source_id": runID, "source_hash": stringValue(run, "state_hash")}, map[string]any{"ordinal": 1, "id": "report", "name": "demo report", "kind": "artifact", "status": "succeeded", "source_kind": "artifact", "source_id": nestedID(artifact, "artifact", "id"), "source_hash": nestedFieldString(artifact, "artifact", "content_hash")}},
		"artifacts": []any{map[string]any{"artifact_id": int64ValueFromString(nestedID(artifact, "artifact", "id")), "workspace_id": session.profile.WorkspaceID, "content_hash": nestedFieldString(artifact, "artifact", "content_hash"), "source_kind": "agent_run", "source_id": runID, "role": "report"}},
		"evidence":  []any{map[string]any{"id": int64ValueFromString(nestedID(evidence, "record", "id")), "workspace_id": session.profile.WorkspaceID, "evidence_hash": nestedFieldString(evidence, "record", "evidence_hash"), "source_reference": "agent-run:" + runID + ":report", "role": "report"}},
	}, false)
	if err != nil {
		return err
	}
	result := map[string]any{"run_id": runID, "receipt_id": nestedID(receipt, "receipt", "id"), "artifact_id": nestedID(artifact, "artifact", "id"), "evidence_id": nestedID(evidence, "record", "id"), "replay_verified": true}
	return printLocalSummary("Fornix demo completed", result, opts.json)
}

func runLocalPrompt(ctx context.Context, opts localOptions) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	repository := opts.repository
	if repository == "" {
		repository = session.profile.RepositoryMount
	}
	if repository == "" {
		return errors.New("a repository is required; use --repo PATH or start Fornix with --repo PATH")
	}
	opts.repository = repository
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	if _, err := manager.Start(ctx); err != nil {
		return fmt.Errorf("prepare repository runtime: %w", err)
	}
	session.profile.ServerURL = manager.ServerURL()
	if err := waitLocalReady(ctx, manager.ServerURL()); err != nil {
		return err
	}
	cli, err := session.operator(opts)
	if err != nil {
		return err
	}
	if err := session.ensureWorkspace(cli, "/workspace/repository"); err != nil {
		return err
	}
	resolved, err := resolveLocalRepository(repository)
	if err != nil {
		return err
	}
	if session.profile.RepositoryMount != resolved {
		session.profile.RepositoryMount = resolved
	}
	if err := session.store.Save(session.profile); err != nil {
		return err
	}
	repositoryName := filepath.Base(resolved)
	prefix := "local-run:" + sha256String(session.profile.WorkspaceID+":"+repositoryName+":"+opts.prompt)
	cli.suppressOutput = true
	workflowArgs := []string{"--workspace", session.profile.WorkspaceID, "--workdir", "/workspace/repository", "--repository", repositoryName, "--goal", opts.prompt, "--provider", opts.provider, "--model", opts.model, "--idempotency-prefix", prefix, "--session", "fornix-cli-" + sha256String(prefix), "--workflow", "fornix-local", "--max-cost", strconv.FormatFloat(opts.maxCost, 'f', -1, 64), "--max-time", opts.maxTime.String(), "--max-turns", strconv.Itoa(opts.maxTurns), "--max-output-tokens", strconv.Itoa(opts.maxOutput), "--max-context-bytes", strconv.Itoa(opts.maxContextB), "--max-context-tokens", strconv.Itoa(opts.maxContextTok)}
	if err := cli.referenceWorkflow(workflowArgs); err != nil {
		return err
	}
	return printWorkflowSummary(cli.lastResponse, opts.json)
}

func runLocalUpgrade(ctx context.Context, opts localOptions) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	pull, err := manager.Pull(ctx)
	if err != nil {
		return err
	}
	up, err := manager.Restart(ctx)
	if err != nil {
		return err
	}
	return printLocalSummary("Fornix runtime upgraded", map[string]any{"pull": pull.Stdout, "restart": up.Stdout}, opts.json)
}

func runLocalUninstall(ctx context.Context, opts localOptions) error {
	session, err := openLocalSession(opts, false)
	if err != nil {
		return err
	}
	if opts.purgeData && !opts.yes {
		return errors.New("--purge-data is destructive; repeat with --yes after confirming the profile and database data should be removed")
	}
	manager, err := session.manager(opts)
	if err != nil {
		return err
	}
	if _, err := manager.Down(ctx, opts.purgeData); err != nil {
		return err
	}
	if opts.purgeData {
		if err := os.RemoveAll(session.store.Root()); err != nil {
			return fmt.Errorf("remove local profile and credentials: %w", err)
		}
		return printLocalSummary("Fornix services and local data removed", map[string]any{"profile": session.store.Root(), "data_removed": true}, opts.json)
	}
	return printLocalSummary("Fornix services stopped; profile and data were preserved", map[string]any{"profile": session.store.Root(), "data_removed": false}, opts.json)
}

func runLocalSupportBundle(ctx context.Context, opts localOptions) error {
	output := opts.output
	if output == "" {
		return errors.New("support requires --output PATH")
	}
	root, err := localProfileRoot(opts.home)
	if err != nil {
		return err
	}
	store, err := profile.New(root)
	if err != nil {
		return err
	}
	p, loadErr := store.Load()
	bundle := map[string]any{"version": version.Current(), "profile_root": store.Root(), "generated_at": time.Now().UTC(), "redacted": true}
	if loadErr == nil {
		bundle["workspace_id"] = p.WorkspaceID
		bundle["server_url"] = p.ServerURL
		bundle["runtime_project"] = p.RuntimeProject
		bundle["runtime_version"] = p.RuntimeVersion
	}
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(output, append(data, '\n'), 0o600); err != nil {
		return err
	}
	_ = ctx
	return printLocalSummary("Redacted support bundle written; it was not uploaded", map[string]any{"path": output, "uploaded": false}, opts.json)
}

func waitLocalReady(ctx context.Context, serverURL string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	delay := 100 * time.Millisecond
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(serverURL, "/")+"/readyz", nil)
		if err == nil {
			response, requestErr := client.Do(req)
			if requestErr == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				_ = response.Body.Close()
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Fornix readiness: %w", ctx.Err())
		case <-deadline.C:
			return errors.New("Fornix did not become ready within 90 seconds; run 'fornix doctor' and 'fornix logs'")
		case <-time.After(delay):
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func printLocalCommandResult(operation string, result localruntime.Result, jsonOutput bool) error {
	if jsonOutput {
		return printLocalJSON(map[string]any{"operation": operation, "exit_code": result.ExitCode, "stdout": result.Stdout, "stderr": result.Stderr})
	}
	fmt.Fprintf(os.Stdout, "%s completed.\n", operation)
	if result.Stdout != "" {
		fmt.Fprintln(os.Stdout, result.Stdout)
	}
	return nil
}

func printDoctorReport(report map[string]any, jsonOutput bool) error {
	if jsonOutput {
		return printLocalJSON(report)
	}
	fmt.Fprintln(os.Stdout, "Fornix doctor")
	checks := report["checks"].(map[string]any)
	for _, name := range []string{"profile", "docker", "configuration", "provider", "repository"} {
		if value, ok := checks[name].(map[string]any); ok {
			fmt.Fprintf(os.Stdout, "%s: %s\n", name, value["status"])
			if message, ok := value["message"].(string); ok && message != "" {
				fmt.Fprintf(os.Stdout, "  %s\n", message)
			}
		}
	}
	return nil
}

func printLocalSummary(title string, result map[string]any, jsonOutput bool) error {
	if jsonOutput {
		return printLocalJSON(result)
	}
	fmt.Fprintln(os.Stdout, title+".")
	for key, value := range result {
		fmt.Fprintf(os.Stdout, "%s: %v\n", key, value)
	}
	return nil
}

func printWorkflowSummary(result map[string]any, jsonOutput bool) error {
	if jsonOutput {
		return printLocalJSON(result)
	}
	run, _ := result["run"].(map[string]any)
	receipt, _ := result["receipt"].(map[string]any)
	artifact, _ := result["artifact"].(map[string]any)
	evidence, _ := result["evidence"].(map[string]any)
	fmt.Fprintln(os.Stdout, "Fornix run completed.")
	fmt.Fprintf(os.Stdout, "Run:       %s\nReceipt:   %s\nReport:    %s\nEvidence:  %s\nReplay:    %v\n", directOrNestedID(run, "run", "id"), directOrNestedID(receipt, "receipt", "id"), directOrNestedID(artifact, "artifact", "id"), directOrNestedID(evidence, "record", "id"), result["replay_verified"])
	return nil
}

func directOrNestedID(value map[string]any, object, key string) string {
	if value == nil {
		return ""
	}
	if direct := stringValue(value, key); direct != "" {
		return direct
	}
	return nestedID(value, object, key)
}

func printLocalJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func randomLocalSecret(prefix string) (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate local secret: %w", err)
	}
	return prefix + hex.EncodeToString(bytes), nil
}

func parseLocalBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func resolveLocalRepository(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve repository: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve repository symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("stat repository: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("repository must be a directory")
	}
	if resolved == string(filepath.Separator) {
		return "", errors.New("repository may not be the filesystem root")
	}
	if home, homeErr := os.UserHomeDir(); homeErr == nil && filepath.Clean(home) == filepath.Clean(resolved) {
		return "", errors.New("repository may not be the home directory")
	}
	return resolved, nil
}
