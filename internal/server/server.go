// Package server exposes Fornix's authenticated HTTP control-plane surface.
// Handlers enforce workspace authorization at the boundary and delegate
// authoritative state, idempotency, and replay behavior to Postgres-backed
// stores.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/omaveda/fornix/internal/agentloop"
	"github.com/omaveda/fornix/internal/config"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/retrieval"
	"github.com/omaveda/fornix/internal/scheduler"
	"github.com/omaveda/fornix/internal/store"
	"github.com/omaveda/fornix/internal/tool"
	"github.com/omaveda/fornix/internal/version"
)

const embeddingDim = 768
const embeddingModel = "nomic-embed-text"

type server struct {
	pool              *pgxpool.Pool
	events            *store.EventStore
	evidence          *store.EvidenceStore
	artifacts         *store.ArtifactStore
	ingests           *store.IngestStore
	tasks             *store.TaskStore
	retrieval         *retrieval.Store
	retrievalSurfaces *store.RetrievalSurfaceStore
	modelRegistry     *model.Registry
	modelGateway      *model.Gateway
	modelCalls        *store.ModelCallStore
	operator          *store.OperatorStore
	observability     *store.ObservabilityStore
	evaluations       *store.EvaluationStore
	toolRegistry      *tool.Registry
	toolExecutor      *tool.Executor
	toolRuns          *store.ToolRunStore
	agentRuns         *store.AgentRunStore
	auth              *store.AuthStore
	agentLoop         *agentloop.Orchestrator
	agentWorker       *scheduler.Worker
	apiKey            string
	bootstrapKey      string
	authMode          string
	workerEnabled     bool
	ollamaURL         string
	httpClient        *http.Client
	maxBodyBytes      int64
	shutdownTimeout   time.Duration
}

// New validates configuration, applies durable migrations, and composes the
// authenticated control-plane services. It does not start listening; callers
// use Run for the server lifecycle.
func New(ctx context.Context, cfg config.Config) (*server, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	poolCfg.MaxConns = cfg.DBMaxConnections
	poolCfg.MinConns = cfg.DBMinConnections
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	events := store.NewEventStore(pool)
	modelCalls := store.NewModelCallStore(pool)
	observability := store.NewObservabilityStore(pool)
	evaluations := store.NewEvaluationStore(pool)
	retrievalSurfaces := store.NewRetrievalSurfaceStore(pool)
	authStore := store.NewAuthStore(pool)
	operatorStore := store.NewOperatorStore(pool, events)
	modelRegistry := model.NewRegistry()
	ollamaProvider, err := model.NewOllamaProvider(model.OllamaConfig{
		Endpoint: contracts.ModelEndpoint{
			ID: "ollama", Provider: "ollama", BaseURL: cfg.OllamaURL,
			DefaultModel: embeddingModel, Enabled: true, AllowPrivate: true,
		},
		EmbeddingModel: embeddingModel, EmbeddingDim: embeddingDim,
		HTTPClient: &http.Client{Timeout: 30 * time.Second}, Timeout: 30 * time.Second,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("configure Ollama provider: %w", err)
	}
	if err := modelRegistry.Register(ollamaProvider); err != nil {
		pool.Close()
		return nil, fmt.Errorf("register Ollama provider: %w", err)
	}
	if err := modelRegistry.Register(model.NewFakeProvider(model.FakeConfig{})); err != nil {
		pool.Close()
		return nil, fmt.Errorf("register fake model provider: %w", err)
	}
	if cfg.OpenAIEnabled {
		openAIProvider, providerErr := model.NewOpenAIProvider(model.OpenAIConfig{
			Endpoint: contracts.ModelEndpoint{
				ID: "openai", Provider: "openai", BaseURL: cfg.OpenAIBaseURL,
				DefaultModel: cfg.OpenAIModel, CredentialRef: cfg.OpenAICredentialRef,
				Enabled: true, AllowPrivate: cfg.OpenAIAllowPrivate,
			},
			RequireAPIKey: true, Timeout: cfg.OpenAITimeout,
			ResolveCredential: func(ref string) (string, error) { return os.Getenv(ref), nil },
		})
		if providerErr != nil {
			pool.Close()
			return nil, fmt.Errorf("configure OpenAI provider: %w", providerErr)
		}
		if err := modelRegistry.Register(openAIProvider); err != nil {
			pool.Close()
			return nil, fmt.Errorf("register OpenAI provider: %w", err)
		}
	}
	toolRegistry := tool.NewRegistry()
	if err := toolRegistry.Register(contracts.ToolDefinition{
		ID: "fornix.echo", Name: "echo", Version: "1", Capability: "process.echo",
		Description: "bounded deterministic argument echo for smoke and offline development",
		Executable:  "/bin/echo", Enabled: true, Sandbox: contracts.DefaultSandboxProfile(),
	}); err != nil {
		pool.Close()
		return nil, fmt.Errorf("register built-in tool: %w", err)
	}
	repositorySandbox := contracts.DefaultSandboxProfile()
	repositorySandbox.ReadOnlyWorkdir = true
	if err := toolRegistry.Register(contracts.ToolDefinition{
		ID: "fornix.repository.read", Name: "repository.read", Version: "1", Capability: "repository.read",
		Description: "read a bounded repository file through structured argv", Executable: "/bin/cat", Enabled: true,
		PathArgvIndexes: []int{1},
		Sandbox:         repositorySandbox,
	}); err != nil {
		pool.Close()
		return nil, fmt.Errorf("register repository tool: %w", err)
	}
	toolPolicy, err := tool.NewPolicy([]contracts.ToolPolicyRule{{
		ID: "builtin-default-echo", Priority: 100, WorkspaceID: contracts.DefaultWorkspaceID,
		ToolID: "fornix.echo", Capability: "process.echo", Mode: contracts.ToolModeAutomatic,
		Enabled: true, Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("configure tool policy: %w", err)
	}
	toolRuns := store.NewToolRunStore(pool, events)
	agentRuns := store.NewAgentRunStore(pool, events)
	modelCalls.SetObservability(observability)
	toolRuns.SetObservability(observability)
	agentRuns.SetObservability(observability)
	modelGateway := model.NewGateway(modelRegistry, modelCalls)
	toolExecutor := &tool.Executor{Registry: toolRegistry, Policy: toolPolicy, Store: toolRuns, Fence: toolRuns}
	retrievalStore := retrieval.NewStore(pool)
	retrievalStore.SetSurfaceRecorder(func(captureCtx context.Context, request contracts.RetrievalRequest, result retrieval.Result, duration time.Duration) error {
		return captureRetrievalSurface(captureCtx, retrievalSurfaces, request, result, duration)
	})
	srv := &server{
		pool:              pool,
		events:            events,
		evidence:          store.NewEvidenceStore(pool),
		artifacts:         store.NewArtifactStore(pool),
		tasks:             store.NewTaskStore(pool, events),
		retrieval:         retrievalStore,
		retrievalSurfaces: retrievalSurfaces,
		modelRegistry:     modelRegistry,
		modelGateway:      modelGateway,
		modelCalls:        modelCalls,
		operator:          operatorStore,
		observability:     observability,
		evaluations:       evaluations,
		toolRegistry:      toolRegistry,
		toolRuns:          toolRuns,
		toolExecutor:      toolExecutor,
		agentRuns:         agentRuns,
		auth:              authStore,
		agentLoop: func() *agentloop.Orchestrator {
			loop := agentloop.New(agentRuns, modelGateway, toolExecutor)
			loop.Approvals = toolRuns
			return loop
		}(),
		apiKey:          cfg.APIKey,
		bootstrapKey:    cfg.BootstrapKey,
		authMode:        cfg.AuthMode,
		workerEnabled:   cfg.WorkerEnabled,
		ollamaURL:       cfg.OllamaURL,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		maxBodyBytes:    cfg.MaxBodyBytes,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
	srv.ingests = store.NewIngestStore(pool, events, srv.artifacts)
	srv.ingests.SetEmbedder(func(embedCtx context.Context, text string) ([]float32, error) {
		return srv.embed(embedCtx, text)
	})
	// Restore workspace-specific read-only repository admission rules from the
	// durable workspace registry. The rule is only an in-process fast path; all
	// requests still require authenticated workspace authorization.
	if page, listErr := operatorStore.ListWorkspaces(ctx, 100, ""); listErr == nil {
		for _, workspace := range page.Items {
			root := workspace.ToolRoot
			if root == "" {
				continue
			}
			_ = toolPolicy.RegisterWorkspaceTool(workspace.ID, "fornix.repository.read", "repository.read", root)
		}
	}
	srv.agentWorker = scheduler.NewWorker(agentRuns, srv.agentLoop, contracts.NewID("server-worker"))
	srv.agentLoop.Retriever = agentloop.ContextRetrieverFunc(func(ctx context.Context, request contracts.RetrievalRequest) (contracts.ContextPack, error) {
		result, err := srv.retrieval.Retrieve(ctx, request)
		if err != nil {
			return contracts.ContextPack{}, err
		}
		return result.Pack, nil
	})
	return srv, nil
}

// Close releases the Postgres pool. It is safe for callers to use during
// shutdown after Run returns.
func (s *server) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Run serves the HTTP API until the context is canceled or the listener fails.
// Middleware applies authentication, workspace scoping, bounded request
// bodies, and authorization before handlers mutate durable state.
func (s *server) Run(ctx context.Context, listen string) error {
	httpServer := &http.Server{
		Addr:              listen,
		Handler:           withRequestMiddleware(s.securityMiddleware(s.routes()), s.maxBodyBytes),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       90 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	bgCtx, cancelBackground := context.WithCancel(ctx)
	defer cancelBackground()
	go s.sessionsReaper(bgCtx)
	go s.federationPoller(bgCtx)
	if s.workerEnabled {
		go func() {
			if err := s.agentWorker.Run(bgCtx, ""); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("agent run worker stopped: %v", err)
			}
		}()
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("fornix v%s listening on %s (ollama=%s, model=%s, dim=%d)", version.Version, listen, s.ollamaURL, embeddingModel, embeddingDim)
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		cancelBackground()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		return nil
	}
}

// ---------- types ----------

type memoCreateReq struct {
	WorkspaceID string   `json:"workspace_id,omitempty"`
	Title       string   `json:"title"`
	Content     string   `json:"content"`
	Type        string   `json:"type"`
	Tags        []string `json:"tags"`
}

type memoUpdateReq struct {
	Title   *string   `json:"title,omitempty"`
	Content *string   `json:"content,omitempty"`
	Type    *string   `json:"type,omitempty"`
	Tags    *[]string `json:"tags,omitempty"`
}

type memoCreateResp struct {
	ID       int64  `json:"id"`
	SHA256   string `json:"sha256"`
	Deduped  bool   `json:"deduped"`
	Embedded bool   `json:"embedded"`
}

type searchReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Query       string `json:"query"`
	TopK        int    `json:"top_k"`
	Type        string `json:"type"`
	Mode        string `json:"mode"` // "hybrid" (default) | "tsvector" | "semantic"
}

type searchHit struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Excerpt   string    `json:"excerpt"`
	Score     float64   `json:"score"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

type coordSendReq struct {
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}

type coordMsg struct {
	ID         int64     `json:"id"`
	Sender     string    `json:"sender"`
	Recipient  string    `json:"recipient"`
	Subject    string    `json:"subject"`
	Body       string    `json:"body"`
	Host       string    `json:"host"`
	OriginHost string    `json:"origin_host"`
	TS         time.Time `json:"ts"`
}

// ---------- v0.5 code graph types ----------

type symbolUpsertReq struct {
	WorkspaceID string  `json:"workspace_id,omitempty"`
	Repo        string  `json:"repo"`
	FilePath    string  `json:"file_path"`
	SymbolName  string  `json:"symbol_name"`
	SymbolKind  string  `json:"symbol_kind"`
	Language    string  `json:"language"`
	LineStart   int     `json:"line_start"`
	LineEnd     int     `json:"line_end"`
	Signature   string  `json:"signature"`
	Docstring   string  `json:"docstring"`
	EmbedSource *string `json:"embed_source,omitempty"` // optional text to embed; defaults to signature+docstring
}

type symbolEdgeReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	SrcID       int64  `json:"src_id"`
	DstID       int64  `json:"dst_id"`
	EdgeKind    string `json:"edge_kind"`
}

type symbolSearchReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Query       string `json:"query"`
	TopK        int    `json:"top_k"`
	Repo        string `json:"repo"`
	Kind        string `json:"symbol_kind"`
	Mode        string `json:"mode"` // hybrid | semantic | name
}

type symbolHit struct {
	ID         int64   `json:"id"`
	Repo       string  `json:"repo"`
	FilePath   string  `json:"file_path"`
	SymbolName string  `json:"symbol_name"`
	SymbolKind string  `json:"symbol_kind"`
	Language   string  `json:"language"`
	LineStart  int     `json:"line_start"`
	LineEnd    int     `json:"line_end"`
	Signature  string  `json:"signature"`
	Score      float64 `json:"score"`
}

// ---------- v0.6 orchestrator types ----------

type sessionRegisterReq struct {
	WorkspaceID  string   `json:"workspace_id,omitempty"`
	ID           string   `json:"id"`
	Host         string   `json:"host"`
	Capabilities []string `json:"capabilities"`
}

type sessionRow struct {
	WorkspaceID   string    `json:"workspace_id"`
	ID            string    `json:"id"`
	Host          string    `json:"host"`
	Capabilities  []string  `json:"capabilities"`
	Status        string    `json:"status"`
	CurrentTaskID *int64    `json:"current_task_id,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	RegisteredAt  time.Time `json:"registered_at"`
}

type taskCreateReq struct {
	WorkspaceID          string   `json:"workspace_id,omitempty"`
	Title                string   `json:"title"`
	Brief                string   `json:"brief"`
	RequiredCapabilities []string `json:"required_capabilities"`
	CreatedBy            string   `json:"created_by"`
	MaxAttempts          int      `json:"max_attempts,omitempty"`
	DependsOn            []int64  `json:"depends_on,omitempty"`
}

type taskClaimReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	SessionID   string `json:"session_id"`
	LeaseTTLMS  int64  `json:"lease_ttl_ms,omitempty"`
}

type taskCompleteReq struct {
	WorkspaceID    string `json:"workspace_id,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	Fence          uint64 `json:"fence,omitempty"`
	Result         string `json:"result"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type agentRunCancelReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type agentRunExternalReq struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Output      string `json:"output,omitempty"`
}

// ---------- v0.7 federation types ----------

type federationPeerReq struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	BearerToken string `json:"bearer_token"`
}

type federationPeerRow struct {
	ID                string     `json:"id"`
	URL               string     `json:"url"`
	LastPullAt        *time.Time `json:"last_pull_at,omitempty"`
	LastPullHighWater int64      `json:"last_pull_high_water"`
}

type federationImportMsg struct {
	Sender     string    `json:"sender"`
	Recipient  string    `json:"recipient"`
	Subject    string    `json:"subject"`
	Body       string    `json:"body"`
	OriginHost string    `json:"origin_host"`
	TS         time.Time `json:"ts"`
}

// ---------- v0.8 router learning types ----------

type routerObservationReq struct {
	RequestHash  string   `json:"request_hash"`
	TaskCategory string   `json:"task_category"`
	ModelID      string   `json:"model_id"`
	CostUSD      float64  `json:"cost_usd"`
	LatencyMs    int      `json:"latency_ms"`
	Outcome      string   `json:"outcome"`
	OutcomeScore *float64 `json:"outcome_score,omitempty"`
}

type routerRecommendation struct {
	ModelID     string  `json:"model_id"`
	CostUSDAvg  float64 `json:"cost_usd_avg"`
	LatencyP50  float64 `json:"latency_p50"`
	SuccessRate float64 `json:"success_rate"`
	SampleSize  int     `json:"sample_size"`
}

func (s *server) embed(ctx context.Context, text string) ([]float32, error) {
	provider, ok := s.modelRegistry.Lookup("ollama")
	if !ok {
		return nil, fmt.Errorf("ollama provider is not registered")
	}
	return provider.Embed(ctx, model.EmbeddingRequest{Model: embeddingModel, Text: text, MaxInputBytes: 2000})
}

// ---------- helpers ----------

func sha256hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *server) requireAuth(r *http.Request) bool {
	if _, ok := principalFromRequest(r); ok {
		return true
	}
	principal, err := s.authenticateRequest(r)
	if err != nil {
		return false
	}
	*r = *withPrincipal(r, principal)
	return validateRequestWorkspace(r, principal) == nil
}

func excerpt(content string, n int) string {
	if len(content) <= n {
		return content
	}
	return content[:n] + "..."
}

func shortError(err error, limit int) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) <= limit {
		return message
	}
	return message[:limit] + "..."
}

func withRequestMiddleware(next http.Handler, maxBodyBytes int64) http.Handler {
	if maxBodyBytes <= 0 {
		maxBodyBytes = 4 << 20
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = newRequestID()
		}
		w.Header().Set("X-Request-ID", requestID)
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		r = r.WithContext(context.WithValue(r.Context(), requestIDContextKey, requestID))
		next.ServeHTTP(w, r)
	})
}

func newRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ---------- handlers ----------

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	dbStatus := "ok"
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		dbStatus = "err: " + shortError(err, 160)
	}
	writeJSON(w, 200, map[string]any{
		"ok":              dbStatus == "ok",
		"version":         version.Version,
		"db":              dbStatus,
		"embedding_model": embeddingModel,
		"embedding_dim":   embeddingDim,
		"model_providers": s.modelRegistry.Names(),
	})
}

func (s *server) handleModelComplete(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.ModelRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid model request: "+shortError(err, 160))
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	request.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	response, err := s.modelGateway.Complete(ctx, request)
	if err != nil {
		s.observeModelRequest(ctx, request)
		writeModelError(w, err)
		return
	}
	s.observeModelRequest(ctx, request)
	writeJSON(w, http.StatusOK, response)
}

func (s *server) handleToolExecute(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.ToolRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid tool request: "+shortError(err, 160))
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	request.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	outcome, err := s.toolExecutor.Execute(ctx, request)
	s.observeToolOutcome(ctx, outcome)
	if err != nil {
		var failureErr *tool.FailureError
		if errors.As(err, &failureErr) {
			writeJSON(w, toolHTTPStatus(failureErr.Failure.Code), map[string]any{
				"outcome": outcome, "error": failureErr.Failure.Message, "code": failureErr.Failure.Code,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, shortError(err, 320))
		return
	}
	writeJSON(w, http.StatusOK, outcome)
}

func (s *server) handleToolApprovalDecision(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	approvalID := strings.TrimPrefix(r.URL.Path, "/v1/tools/approvals/")
	approvalID = strings.TrimSuffix(approvalID, "/decide")
	if strings.TrimSpace(approvalID) == "" {
		writeErr(w, http.StatusBadRequest, "approval id required")
		return
	}
	var decision contracts.ApprovalDecision
	if err := json.NewDecoder(r.Body).Decode(&decision); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid approval decision: "+shortError(err, 160))
		return
	}
	decision.ApprovalID = approvalID
	decision.WorkspaceID = requestWorkspace(r, decision.WorkspaceID)
	decision.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	approval, err := s.toolRuns.DecideApproval(ctx, decision)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, store.ErrToolApprovalMissing) {
			status = http.StatusNotFound
		}
		writeErr(w, status, shortError(err, 320))
		return
	}
	writeJSON(w, http.StatusOK, approval)
}

func (s *server) handleAgentRunCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.AgentRunRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid agent run request: "+shortError(err, 160))
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	request.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	run, deduplicated, err := s.agentLoop.Create(ctx, request)
	if err != nil {
		writeAgentRunError(w, err)
		return
	}
	decision, runErr := s.agentLoop.Run(ctx, run.WorkspaceID, run.ID)
	s.observeAgentOutcome(ctx, decision.Run)
	if runErr != nil {
		writeJSON(w, agentRunHTTPStatus(runErr), map[string]any{"run": run, "decision": decision, "deduplicated": deduplicated, "error": shortError(runErr, 320)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": decision.Run, "decision": decision, "deduplicated": deduplicated})
}

func (s *server) handleAgentRunAdvance(w http.ResponseWriter, r *http.Request, runID string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	decision, err := s.agentLoop.Advance(ctx, workspaceID, runID)
	s.observeAgentOutcome(ctx, decision.Run)
	if err != nil {
		writeJSON(w, agentRunHTTPStatus(err), map[string]any{"decision": decision, "error": shortError(err, 320)})
		return
	}
	writeJSON(w, http.StatusOK, decision)
}

func (s *server) handleAgentRunGet(w http.ResponseWriter, r *http.Request, runID string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	run, err := s.agentRuns.Get(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), runID)
	if err != nil {
		writeAgentRunError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *server) handleAgentRunCancel(w http.ResponseWriter, r *http.Request, runID string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request agentRunCancelReq
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid cancellation request: "+shortError(err, 160))
		return
	}
	workspaceRef := request.WorkspaceID
	if strings.TrimSpace(workspaceRef) == "" {
		workspaceRef = r.URL.Query().Get("workspace_id")
	}
	workspaceID := requestWorkspace(r, workspaceRef)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	decision, err := s.agentLoop.Cancel(ctx, workspaceID, runID, request.Reason)
	s.observeAgentOutcome(ctx, decision.Run)
	if err != nil {
		writeJSON(w, agentRunHTTPStatus(err), map[string]any{"decision": decision, "error": shortError(err, 320)})
		return
	}
	writeJSON(w, http.StatusOK, decision)
}

func (s *server) handleAgentRunExternal(w http.ResponseWriter, r *http.Request, runID, operation string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request agentRunExternalReq
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid external completion request: "+shortError(err, 160))
		return
	}
	workspaceRef := request.WorkspaceID
	if strings.TrimSpace(workspaceRef) == "" {
		workspaceRef = r.URL.Query().Get("workspace_id")
	}
	workspaceID := requestWorkspace(r, workspaceRef)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var (
		decision contracts.LoopDecision
		err      error
	)
	if operation == "wait" {
		decision, err = s.agentLoop.WaitExternal(ctx, workspaceID, runID, request.Reason)
	} else {
		decision, err = s.agentLoop.CompleteExternal(ctx, workspaceID, runID, request.Output)
	}
	s.observeAgentOutcome(ctx, decision.Run)
	if err != nil {
		writeJSON(w, agentRunHTTPStatus(err), map[string]any{"decision": decision, "error": shortError(err, 320)})
		return
	}
	writeJSON(w, http.StatusOK, decision)
}

func (s *server) handleAgentRunReplay(w http.ResponseWriter, r *http.Request, runID string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	from, _ := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("from")), 10, 64)
	limit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
	if limit <= 0 {
		limit = 500
	}
	events, err := s.agentRuns.Replay(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), runID, from, limit)
	if err != nil {
		writeAgentRunError(w, err)
		return
	}
	response := map[string]any{"run_id": runID, "events": events, "count": len(events)}
	if checkpoint, checkpointErr := s.agentRuns.ReplayCheckpoint(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), runID, from, limit); checkpointErr == nil {
		response["checkpoint"] = checkpoint
	}
	writeJSON(w, http.StatusOK, response)
}

func writeAgentRunError(w http.ResponseWriter, err error) {
	writeJSON(w, agentRunHTTPStatus(err), map[string]string{"error": shortError(err, 320)})
}

func agentRunHTTPStatus(err error) int {
	switch {
	case errors.Is(err, store.ErrAgentRunMissing):
		return http.StatusNotFound
	case errors.Is(err, store.ErrAgentRunConflict), errors.Is(err, store.ErrAgentRunTerminal), errors.Is(err, store.ErrAgentRunCancelled):
		return http.StatusConflict
	case errors.Is(err, store.ErrAgentRunStale):
		return http.StatusGone
	case errors.Is(err, agentloop.ErrLoopWaiting):
		return http.StatusAccepted
	default:
		return http.StatusBadGateway
	}
}

func toolHTTPStatus(code string) int {
	switch code {
	case contracts.ToolFailureInvalidRequest, contracts.ToolFailureArgumentLimit, contracts.ToolFailureEnvironmentLimit, contracts.ToolFailureWorkdirDenied:
		return http.StatusBadRequest
	case contracts.ToolFailureUnauthorized, contracts.ToolFailureApprovalDenied, contracts.ToolFailureApprovalExpired:
		return http.StatusForbidden
	case contracts.ToolFailureApprovalRequired:
		return http.StatusAccepted
	case contracts.ToolFailureInProgress, contracts.ToolFailureConflict, contracts.ToolFailureStaleFence:
		return http.StatusConflict
	case contracts.ToolFailureTimeout:
		return http.StatusGatewayTimeout
	case contracts.ToolFailureOutputLimit:
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusBadGateway
	}
}

func writeModelError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	message := shortError(err, 320)
	var failureErr *model.FailureError
	if errors.As(err, &failureErr) {
		message = excerpt(failureErr.Failure.Message, 320)
		switch failureErr.Failure.Code {
		case contracts.ModelFailureAuthentication:
			status = http.StatusUnauthorized
		case contracts.ModelFailureRateLimit:
			status = http.StatusTooManyRequests
		case contracts.ModelFailureQuota:
			status = http.StatusPaymentRequired
		case contracts.ModelFailureInvalidRequest, contracts.ModelFailureContextWindow, contracts.ModelFailureBudget:
			status = http.StatusBadRequest
		case contracts.ModelFailureTimeout:
			status = http.StatusGatewayTimeout
		case contracts.ModelFailureCancelled:
			status = http.StatusRequestTimeout
		}
	} else if errors.Is(err, model.ErrModelCallInFlight) {
		status = http.StatusConflict
		message = "model call is already in progress"
	}
	writeJSON(w, status, map[string]any{"error": message, "code": modelErrorCode(err)})
}

func modelErrorCode(err error) string {
	var failureErr *model.FailureError
	if errors.As(err, &failureErr) {
		return failureErr.Failure.Code
	}
	if errors.Is(err, model.ErrModelCallInFlight) {
		return contracts.ModelFailureInProgress
	}
	return "model_execution"
}

func (s *server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "version": version.Version})
}

func (s *server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":      false,
			"version": version.Version,
			"db":      shortError(err, 160),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "version": version.Version, "db": "ok"})
}

func (s *server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req memoCreateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json: "+err.Error())
		return
	}
	if req.Content == "" {
		writeErr(w, 400, "content required")
		return
	}
	if req.Type == "" {
		req.Type = "general"
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	hash := sha256hex(req.Title + "\n" + req.Content)

	// Try insert; on conflict return existing id
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// Generate embedding (best-effort; on failure store NULL)
	var emb []float32
	if e, err := s.embed(ctx, req.Title+"\n"+req.Content); err == nil {
		emb = e
	} else {
		log.Printf("embed warn (memo): %v", err)
	}

	var id int64
	var deduped bool
	var inserted bool
	if emb != nil {
		err := s.pool.QueryRow(ctx, `
			INSERT INTO fornix.memos (workspace_id, title, content, type, tags, sha256, embedding)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (workspace_id, sha256) DO UPDATE SET sha256 = EXCLUDED.sha256
			RETURNING id, (xmax = 0)`,
			workspaceID, req.Title, req.Content, req.Type, append([]string{}, req.Tags...), hash, pgvector.NewVector(emb),
		).Scan(&id, &inserted)
		if err != nil {
			writeErr(w, 500, "db: "+err.Error())
			return
		}
	} else {
		err := s.pool.QueryRow(ctx, `
			INSERT INTO fornix.memos (workspace_id, title, content, type, tags, sha256)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (workspace_id, sha256) DO UPDATE SET sha256 = EXCLUDED.sha256
			RETURNING id, (xmax = 0)`,
			workspaceID, req.Title, req.Content, req.Type, append([]string{}, req.Tags...), hash,
		).Scan(&id, &inserted)
		if err != nil {
			writeErr(w, 500, "db: "+err.Error())
			return
		}
	}
	deduped = !inserted
	writeJSON(w, 200, memoCreateResp{ID: id, SHA256: hash, Deduped: deduped, Embedded: emb != nil})
}

func (s *server) handleUpdate(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req memoUpdateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// Read existing
	var title, content, mtype string
	var tags []string
	workspaceID := requestWorkspace(r, "")
	err := s.pool.QueryRow(ctx, `SELECT title, content, type, tags FROM fornix.memos WHERE workspace_id=$1 AND id=$2 AND deleted_at IS NULL`, workspaceID, id).Scan(&title, &content, &mtype, &tags)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, "not found")
			return
		}
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	if req.Title != nil {
		title = *req.Title
	}
	if req.Content != nil {
		content = *req.Content
	}
	if req.Type != nil {
		mtype = *req.Type
	}
	if req.Tags != nil {
		tags = *req.Tags
	}
	hash := sha256hex(title + "\n" + content)
	// Re-embed
	var emb []float32
	if e, err := s.embed(ctx, title+"\n"+content); err == nil {
		emb = e
	}
	if emb != nil {
		_, err = s.pool.Exec(ctx, `UPDATE fornix.memos SET title=$1, content=$2, type=$3, tags=$4, sha256=$5, embedding=$6, updated_at=now() WHERE workspace_id=$7 AND id=$8`,
			title, content, mtype, tags, hash, pgvector.NewVector(emb), workspaceID, id)
	} else {
		_, err = s.pool.Exec(ctx, `UPDATE fornix.memos SET title=$1, content=$2, type=$3, tags=$4, sha256=$5, updated_at=now() WHERE workspace_id=$6 AND id=$7`,
			title, content, mtype, tags, hash, workspaceID, id)
	}
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "sha256": hash, "embedded": emb != nil})
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, "")
	tag, err := s.pool.Exec(ctx, `UPDATE fornix.memos SET deleted_at=now() WHERE workspace_id=$1 AND id=$2 AND deleted_at IS NULL`, workspaceID, id)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeErr(w, 404, "not found or already deleted")
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "deleted": true})
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, "")
	var title, content, mtype string
	var tags []string
	var createdAt, updatedAt time.Time
	err := s.pool.QueryRow(ctx, `SELECT title, content, type, tags, created_at, updated_at FROM fornix.memos WHERE workspace_id=$1 AND id=$2 AND deleted_at IS NULL`, workspaceID, id).Scan(&title, &content, &mtype, &tags, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, "not found")
			return
		}
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"id": id, "title": title, "content": content, "type": mtype, "tags": tags,
		"created_at": createdAt, "updated_at": updatedAt,
	})
}

func (s *server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req searchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.Query == "" {
		writeErr(w, 400, "query required")
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	if req.Mode == "" {
		req.Mode = "hybrid"
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	// Compute query embedding for semantic + hybrid modes
	var qEmb []float32
	if req.Mode == "semantic" || req.Mode == "hybrid" {
		if e, err := s.embed(ctx, req.Query); err == nil {
			qEmb = e
		} else {
			log.Printf("embed warn (search): %v", err)
			// fall back to tsvector if embedding fails
			req.Mode = "tsvector"
		}
	}

	var rows pgx.Rows
	var err error
	switch req.Mode {
	case "tsvector":
		rows, err = s.pool.Query(ctx, `
			SELECT id, title, content,
			       ts_rank(tsv, plainto_tsquery('english', $1)) * 0.7
			       + (1.0 / (1 + EXTRACT(EPOCH FROM (now()-created_at))/86400.0/30)) * 0.3 AS score,
			       type, created_at
			FROM fornix.memos
			WHERE workspace_id=$3 AND deleted_at IS NULL
			  AND tsv @@ plainto_tsquery('english', $1)
			  AND ($2 = '' OR type = $2)
			ORDER BY score DESC, id LIMIT $4`,
			req.Query, req.Type, workspaceID, req.TopK)
	case "semantic":
		rows, err = s.pool.Query(ctx, `
			SELECT id, title, content,
			       (1.0 - (embedding <=> $1)) * 0.8
			       + (1.0 / (1 + EXTRACT(EPOCH FROM (now()-created_at))/86400.0/30)) * 0.2 AS score,
			       type, created_at
			FROM fornix.memos
			WHERE workspace_id=$2 AND deleted_at IS NULL
			  AND embedding IS NOT NULL
			  AND ($3 = '' OR type = $3)
			ORDER BY embedding <=> $1 ASC, id LIMIT $4`,
			pgvector.NewVector(qEmb), workspaceID, req.Type, req.TopK)
	default: // hybrid
		rows, err = s.pool.Query(ctx, `
			SELECT id, title, content,
			       CASE WHEN embedding IS NULL THEN
			           ts_rank(tsv, plainto_tsquery('english', $2)) * 0.7
			           + (1.0 / (1 + EXTRACT(EPOCH FROM (now()-created_at))/86400.0/30)) * 0.3
			       ELSE
			           (1.0 - (embedding <=> $1)) * 0.5
			           + COALESCE(ts_rank(tsv, plainto_tsquery('english', $2)), 0) * 0.3
			           + (1.0 / (1 + EXTRACT(EPOCH FROM (now()-created_at))/86400.0/30)) * 0.2
			       END AS score,
			       type, created_at
			FROM fornix.memos
			WHERE workspace_id=$4 AND deleted_at IS NULL
			  AND ($3 = '' OR type = $3)
			  AND (
			      embedding IS NOT NULL
			      OR tsv @@ plainto_tsquery('english', $2)
			  )
			ORDER BY score DESC, id LIMIT $5`,
			pgvector.NewVector(qEmb), req.Query, req.Type, workspaceID, req.TopK)
	}
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()

	results := []searchHit{}
	for rows.Next() {
		var h searchHit
		var content string
		if err := rows.Scan(&h.ID, &h.Title, &content, &h.Score, &h.Type, &h.CreatedAt); err != nil {
			continue
		}
		h.Excerpt = excerpt(content, 200)
		results = append(results, h)
	}
	writeJSON(w, 200, map[string]any{"results": results, "mode": req.Mode})
}

// ---------- coord endpoints ----------

func (s *server) handleCoordSend(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req coordSendReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.Sender == "" || req.Recipient == "" || req.Subject == "" {
		writeErr(w, 400, "sender, recipient, subject required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	host, _ := os.Hostname()
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO public.coord_messages (sender, recipient, subject, body, host, origin_host)
		VALUES ($1, $2, $3, $4, $5, $5)
		RETURNING id`,
		req.Sender, req.Recipient, req.Subject, req.Body, host,
	).Scan(&id)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	// pg_notify trigger should already fire; do it explicitly too for safety
	_, _ = s.pool.Exec(ctx, `SELECT pg_notify('coord', $1)`, fmt.Sprintf(`{"id":%d,"sender":%q,"recipient":%q,"subject":%q}`, id, req.Sender, req.Recipient, req.Subject))
	writeJSON(w, 200, map[string]any{"id": id, "sent": true})
}

func (s *server) handleCoordRecent(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	since := r.URL.Query().Get("since") // RFC3339 timestamp
	recipient := r.URL.Query().Get("recipient")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := `SELECT id, sender, recipient, subject, body, COALESCE(host,''), COALESCE(origin_host,'local'), ts FROM public.coord_messages WHERE 1=1`
	args := []any{}
	if since != "" {
		args = append(args, since)
		query += fmt.Sprintf(" AND ts > $%d", len(args))
	}
	if recipient != "" {
		args = append(args, recipient)
		query += fmt.Sprintf(" AND (recipient = $%d OR recipient = 'all' OR recipient = 'ALL')", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY ts DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	msgs := []coordMsg{}
	for rows.Next() {
		var m coordMsg
		if err := rows.Scan(&m.ID, &m.Sender, &m.Recipient, &m.Subject, &m.Body, &m.Host, &m.OriginHost, &m.TS); err != nil {
			continue
		}
		msgs = append(msgs, m)
	}
	writeJSON(w, 200, map[string]any{"messages": msgs, "count": len(msgs)})
}

// ---------- backfill ----------

func (s *server) handleBackfillEmbeddings(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()

	rows, err := s.pool.Query(ctx, `SELECT id, title, content FROM fornix.memos WHERE embedding IS NULL AND deleted_at IS NULL ORDER BY id`)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	type todo struct {
		ID      int64
		Title   string
		Content string
	}
	var pending []todo
	for rows.Next() {
		var t todo
		if err := rows.Scan(&t.ID, &t.Title, &t.Content); err != nil {
			continue
		}
		pending = append(pending, t)
	}
	rows.Close()

	stats := map[string]int{"total": len(pending), "ok": 0, "fail": 0}
	for _, t := range pending {
		ec, ecancel := context.WithTimeout(ctx, 15*time.Second)
		emb, err := s.embed(ec, t.Title+"\n"+t.Content)
		ecancel()
		if err != nil {
			stats["fail"]++
			continue
		}
		if _, err := s.pool.Exec(ctx, `UPDATE fornix.memos SET embedding=$1 WHERE id=$2`, pgvector.NewVector(emb), t.ID); err != nil {
			stats["fail"]++
			continue
		}
		stats["ok"]++
	}
	writeJSON(w, 200, stats)
}

// ---------- v0.5 code graph handlers ----------

func (s *server) handleSymbolUpsert(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req symbolUpsertReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json: "+err.Error())
		return
	}
	if req.Repo == "" || req.FilePath == "" || req.SymbolName == "" || req.SymbolKind == "" || req.Language == "" {
		writeErr(w, 400, "repo, file_path, symbol_name, symbol_kind, language required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, req.WorkspaceID)

	embedText := req.Signature + "\n" + req.Docstring
	if req.EmbedSource != nil {
		embedText = *req.EmbedSource
	}
	hash := sha256hex(fmt.Sprintf("%s|%s|%s|%s|%d|%d|%s", req.Repo, req.FilePath, req.SymbolName, req.SymbolKind, req.LineStart, req.LineEnd, req.Signature))

	var emb []float32
	if strings.TrimSpace(embedText) != "" {
		if e, err := s.embed(ctx, embedText); err == nil {
			emb = e
		} else {
			log.Printf("embed warn (symbol): %v", err)
		}
	}

	var id int64
	var inserted bool
	if emb != nil {
		err := s.pool.QueryRow(ctx, `
			INSERT INTO fornix.symbols (workspace_id, repo, file_path, symbol_name, symbol_kind, language, line_start, line_end, signature, docstring, embedding, sha256)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (workspace_id, repo, file_path, symbol_name, symbol_kind) DO UPDATE
			  SET language=EXCLUDED.language,
			      line_start=EXCLUDED.line_start,
			      line_end=EXCLUDED.line_end,
			      signature=EXCLUDED.signature,
			      docstring=EXCLUDED.docstring,
			      embedding=EXCLUDED.embedding,
			      sha256=EXCLUDED.sha256,
			      updated_at=now(),
			      deleted_at=NULL
			RETURNING id, (xmax = 0)`,
			workspaceID, req.Repo, req.FilePath, req.SymbolName, req.SymbolKind, req.Language,
			req.LineStart, req.LineEnd, req.Signature, req.Docstring, pgvector.NewVector(emb), hash,
		).Scan(&id, &inserted)
		if err != nil {
			writeErr(w, 500, "db: "+err.Error())
			return
		}
	} else {
		err := s.pool.QueryRow(ctx, `
			INSERT INTO fornix.symbols (workspace_id, repo, file_path, symbol_name, symbol_kind, language, line_start, line_end, signature, docstring, sha256)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (workspace_id, repo, file_path, symbol_name, symbol_kind) DO UPDATE
			  SET language=EXCLUDED.language,
			      line_start=EXCLUDED.line_start,
			      line_end=EXCLUDED.line_end,
			      signature=EXCLUDED.signature,
			      docstring=EXCLUDED.docstring,
			      sha256=EXCLUDED.sha256,
			      updated_at=now(),
			      deleted_at=NULL
			RETURNING id, (xmax = 0)`,
			workspaceID, req.Repo, req.FilePath, req.SymbolName, req.SymbolKind, req.Language,
			req.LineStart, req.LineEnd, req.Signature, req.Docstring, hash,
		).Scan(&id, &inserted)
		if err != nil {
			writeErr(w, 500, "db: "+err.Error())
			return
		}
	}
	writeJSON(w, 200, map[string]any{"id": id, "inserted": inserted, "embedded": emb != nil})
}

func (s *server) handleSymbolEdge(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req symbolEdgeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.SrcID == 0 || req.DstID == 0 || req.EdgeKind == "" {
		writeErr(w, 400, "src_id, dst_id, edge_kind required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO fornix.symbol_edges (src_id, dst_id, edge_kind)
		SELECT $1, $2, $3
		WHERE EXISTS (SELECT 1 FROM fornix.symbols WHERE id=$1 AND workspace_id=$4)
		  AND EXISTS (SELECT 1 FROM fornix.symbols WHERE id=$2 AND workspace_id=$4)
		ON CONFLICT DO NOTHING`, req.SrcID, req.DstID, req.EdgeKind, workspaceID)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *server) handleSymbolSearch(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req symbolSearchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.Query == "" {
		writeErr(w, 400, "query required")
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	if req.Mode == "" {
		req.Mode = "hybrid"
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	var qEmb []float32
	if req.Mode == "semantic" || req.Mode == "hybrid" {
		if e, err := s.embed(ctx, req.Query); err == nil {
			qEmb = e
		} else {
			log.Printf("embed warn (symbol search): %v", err)
			req.Mode = "name"
		}
	}

	var rows pgx.Rows
	var err error
	switch req.Mode {
	case "name":
		rows, err = s.pool.Query(ctx, `
			SELECT id, repo, file_path, symbol_name, symbol_kind, language, line_start, line_end, COALESCE(signature,''),
			       CASE WHEN symbol_name = $1 THEN 1.0
			            WHEN symbol_name ILIKE $1 || '%' THEN 0.85
			            WHEN symbol_name ILIKE '%' || $1 || '%' THEN 0.6
			            ELSE 0.3 END AS score
			FROM fornix.symbols
			WHERE workspace_id=$4 AND deleted_at IS NULL
			  AND ($2 = '' OR repo = $2)
			  AND ($3 = '' OR symbol_kind = $3)
			  AND symbol_name ILIKE '%' || $1 || '%'
			ORDER BY score DESC, symbol_name, id LIMIT $5`,
			req.Query, req.Repo, req.Kind, workspaceID, req.TopK)
	case "semantic":
		rows, err = s.pool.Query(ctx, `
			SELECT id, repo, file_path, symbol_name, symbol_kind, language, line_start, line_end, COALESCE(signature,''),
			       (1.0 - (embedding <=> $1)) AS score
			FROM fornix.symbols
			WHERE workspace_id=$2 AND deleted_at IS NULL
			  AND embedding IS NOT NULL
			  AND ($3 = '' OR repo = $3)
			  AND ($4 = '' OR symbol_kind = $4)
			ORDER BY embedding <=> $1 ASC, id LIMIT $5`,
			pgvector.NewVector(qEmb), workspaceID, req.Repo, req.Kind, req.TopK)
	default: // hybrid: exact-name dominates; otherwise blend semantic + name signal
		rows, err = s.pool.Query(ctx, `
			SELECT id, repo, file_path, symbol_name, symbol_kind, language, line_start, line_end, COALESCE(signature,''),
			       CASE
			         WHEN symbol_name = $2 THEN
			           0.9 + COALESCE((1.0 - (embedding <=> $1)) * 0.1, 0.1)
			         WHEN symbol_name ILIKE $2 || '%' THEN
			           0.7 + COALESCE((1.0 - (embedding <=> $1)) * 0.2, 0.1)
			         WHEN symbol_name ILIKE '%' || $2 || '%' THEN
			           0.5 + COALESCE((1.0 - (embedding <=> $1)) * 0.3, 0.1)
			         WHEN embedding IS NOT NULL THEN
			           (1.0 - (embedding <=> $1)) * 0.6
			         ELSE 0.3
			       END AS score
			FROM fornix.symbols
			WHERE workspace_id=$5 AND deleted_at IS NULL
			  AND ($3 = '' OR repo = $3)
			  AND ($4 = '' OR symbol_kind = $4)
			  AND (embedding IS NOT NULL OR symbol_name ILIKE '%' || $2 || '%')
			ORDER BY score DESC, symbol_name, id LIMIT $6`,
			pgvector.NewVector(qEmb), req.Query, req.Repo, req.Kind, workspaceID, req.TopK)
	}
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	hits := []symbolHit{}
	for rows.Next() {
		var h symbolHit
		if err := rows.Scan(&h.ID, &h.Repo, &h.FilePath, &h.SymbolName, &h.SymbolKind, &h.Language, &h.LineStart, &h.LineEnd, &h.Signature, &h.Score); err != nil {
			continue
		}
		hits = append(hits, h)
	}
	writeJSON(w, 200, map[string]any{"results": hits, "mode": req.Mode})
}

func (s *server) handleSymbolNeighbours(w http.ResponseWriter, r *http.Request, id int64, direction string) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, "")

	var query string
	if direction == "callers" {
		// who calls this symbol → src for edges pointing at id
		query = `
			SELECT s.id, s.repo, s.file_path, s.symbol_name, s.symbol_kind, s.language, s.line_start, s.line_end, COALESCE(s.signature,''),
			       e.edge_kind
			FROM fornix.symbol_edges e
			JOIN fornix.symbols root ON root.id = e.dst_id AND root.workspace_id = $2
			JOIN fornix.symbols s ON s.id = e.src_id AND s.workspace_id = $2
			WHERE e.dst_id = $1 AND s.deleted_at IS NULL
			ORDER BY s.repo, s.file_path, s.symbol_name, s.id`
	} else {
		// callees: what this symbol calls
		query = `
			SELECT s.id, s.repo, s.file_path, s.symbol_name, s.symbol_kind, s.language, s.line_start, s.line_end, COALESCE(s.signature,''),
			       e.edge_kind
			FROM fornix.symbol_edges e
			JOIN fornix.symbols root ON root.id = e.src_id AND root.workspace_id = $2
			JOIN fornix.symbols s ON s.id = e.dst_id AND s.workspace_id = $2
			WHERE e.src_id = $1 AND s.deleted_at IS NULL
			ORDER BY s.repo, s.file_path, s.symbol_name, s.id`
	}
	rows, err := s.pool.Query(ctx, query, id, workspaceID)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	type neighbour struct {
		symbolHit
		EdgeKind string `json:"edge_kind"`
	}
	out := []neighbour{}
	for rows.Next() {
		var n neighbour
		if err := rows.Scan(&n.ID, &n.Repo, &n.FilePath, &n.SymbolName, &n.SymbolKind, &n.Language, &n.LineStart, &n.LineEnd, &n.Signature, &n.EdgeKind); err != nil {
			continue
		}
		out = append(out, n)
	}
	writeJSON(w, 200, map[string]any{"results": out, "direction": direction, "count": len(out)})
}

func (s *server) handleSymbolReindex(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	// Body: {repo, file_path} — soft-delete any symbols in that file so the indexer can rewrite cleanly.
	var req struct {
		Repo     string `json:"repo"`
		FilePath string `json:"file_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.Repo == "" || req.FilePath == "" {
		writeErr(w, 400, "repo and file_path required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, "")
	tag, err := s.pool.Exec(ctx, `UPDATE fornix.symbols SET deleted_at = now() WHERE workspace_id=$1 AND repo=$2 AND file_path=$3 AND deleted_at IS NULL`, workspaceID, req.Repo, req.FilePath)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"cleared": tag.RowsAffected(), "repo": req.Repo, "file_path": req.FilePath})
}

// ---------- v0.6 orchestrator handlers ----------

func (s *server) handleSessionRegister(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req sessionRegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.ID == "" || req.Host == "" {
		writeErr(w, 400, "id and host required")
		return
	}
	if req.Capabilities == nil {
		req.Capabilities = []string{}
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var registeredWorkspace string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO fornix.sessions (workspace_id, id, host, capabilities, status, last_heartbeat, registered_at)
		VALUES ($1,$2,$3,$4,'idle', clock_timestamp(), clock_timestamp())
		ON CONFLICT (workspace_id, id) DO UPDATE SET
		  host=EXCLUDED.host,
		  capabilities=EXCLUDED.capabilities,
		  status=CASE WHEN fornix.sessions.status='offline' THEN 'idle' ELSE fornix.sessions.status END,
		  last_heartbeat=clock_timestamp()
		RETURNING workspace_id`,
		workspaceID, req.ID, req.Host, req.Capabilities).Scan(&registeredWorkspace)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 409, "session id belongs to another workspace")
			return
		}
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"id": req.ID, "workspace_id": registeredWorkspace, "registered": true})
}

func (s *server) handleSessionHeartbeat(w http.ResponseWriter, r *http.Request, id string) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, "")
	tag, err := s.pool.Exec(ctx, `
		UPDATE fornix.sessions
		SET last_heartbeat=clock_timestamp(),
		    status=CASE WHEN status='offline' THEN 'idle' ELSE status END
		WHERE workspace_id=$1 AND id=$2`, workspaceID, id)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeErr(w, 404, "session not registered")
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "heartbeat": time.Now().UTC()})
}

func (s *server) handleSessionsList(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	statusFilter := r.URL.Query().Get("status")
	capability := r.URL.Query().Get("capability")
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))

	query := `SELECT workspace_id, id, host, capabilities,
	                 CASE WHEN last_heartbeat < now() - INTERVAL '90 seconds' THEN 'offline' ELSE status END AS effective_status,
	                 current_task_id, last_heartbeat, registered_at
	          FROM fornix.sessions WHERE workspace_id=$1`
	args := []any{workspaceID}
	if statusFilter != "" {
		args = append(args, statusFilter)
		query += fmt.Sprintf(" AND (CASE WHEN last_heartbeat < now() - INTERVAL '90 seconds' THEN 'offline' ELSE status END) = $%d", len(args))
	}
	if capability != "" {
		args = append(args, capability)
		query += fmt.Sprintf(" AND $%d = ANY(capabilities)", len(args))
	}
	query += " ORDER BY last_heartbeat DESC"
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	out := []sessionRow{}
	for rows.Next() {
		var sr sessionRow
		var current *int64
		if err := rows.Scan(&sr.WorkspaceID, &sr.ID, &sr.Host, &sr.Capabilities, &sr.Status, &current, &sr.LastHeartbeat, &sr.RegisteredAt); err != nil {
			continue
		}
		sr.CurrentTaskID = current
		out = append(out, sr)
	}
	writeJSON(w, 200, map[string]any{"sessions": out, "count": len(out)})
}

func (s *server) handleTaskCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req taskCreateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if principal, ok := principalFromRequest(r); ok {
		req.CreatedBy = principal.ID
	}
	if req.Title == "" || req.Brief == "" || req.CreatedBy == "" {
		writeErr(w, 400, "title, brief, created_by required")
		return
	}
	if req.RequiredCapabilities == nil {
		req.RequiredCapabilities = []string{}
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	host, _ := os.Hostname()
	task, event, err := s.tasks.Create(ctx, store.TaskCreateInput{
		WorkspaceID: workspaceID, Title: req.Title, Brief: req.Brief,
		RequiredCapabilities: req.RequiredCapabilities, CreatedBy: req.CreatedBy,
		OriginHost: host, MaxAttempts: req.MaxAttempts, DependsOn: req.DependsOn,
	})
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": task.ID, "workspace_id": workspaceID, "created": true, "event_id": event.EventID, "event_sequence": event.Sequence})
}

func (s *server) handleTaskClaim(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req taskClaimReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.SessionID == "" {
		writeErr(w, 400, "session_id required")
		return
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.tasks.ClaimNext(ctx, store.TaskClaimInput{WorkspaceID: workspaceID, SessionID: req.SessionID, ActorID: requestActor(r).ID, LeaseTTL: time.Duration(req.LeaseTTLMS) * time.Millisecond})
	if err != nil {
		if errors.Is(err, store.ErrTaskNoReady) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, 200, struct {
		store.Task
		Lease    contracts.TaskExecutionLease `json:"lease"`
		Fence    uint64                       `json:"fence"`
		Takeover bool                         `json:"takeover"`
	}{Task: result.Task, Lease: result.Lease, Fence: result.Lease.Fence, Takeover: result.Takeover})
}

func (s *server) handleTaskComplete(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	var req taskCompleteReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ownerID, fence := taskOwnerAndFence(r, req.SessionID, req.Fence)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if strings.TrimSpace(req.Status) == contracts.TaskStatusFailed {
		result, err := s.tasks.Fail(ctx, store.TaskFailureInput{WorkspaceID: workspaceID, TaskID: id, OwnerID: ownerID, Fence: fence, Error: req.Result, FailureClass: contracts.FailureUnknown, Retryable: boolPtr(false), IdempotencyKey: req.IdempotencyKey, ActorID: requestActor(r).ID, Payload: body})
		if err != nil {
			writeTaskStoreErr(w, err)
			return
		}
		writeTaskMutationResponse(w, id, result)
		return
	}
	result, err := s.tasks.Complete(ctx, store.TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: id, OwnerID: ownerID, Fence: fence, Result: req.Result, Status: req.Status, IdempotencyKey: req.IdempotencyKey, ActorID: requestActor(r).ID, Payload: body})
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"id":             id,
		"status":         result.Task.Status,
		"event_id":       result.Event.EventID,
		"event_sequence": result.Event.Sequence,
		"deduped":        result.Deduped,
	})
}

func (s *server) handleTasksList(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	statusFilter := r.URL.Query().Get("status")
	assigned := r.URL.Query().Get("assigned")
	since := r.URL.Query().Get("since")
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	tasks, err := s.tasks.List(ctx, workspaceID, statusFilter, assigned, since, 200)
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"tasks": tasks, "count": len(tasks), "workspace_id": workspaceID})
}

func (s *server) handleTaskGet(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	task, err := s.tasks.Get(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), id)
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *server) handleTaskRenew(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req struct {
		WorkspaceID string `json:"workspace_id,omitempty"`
		SessionID   string `json:"session_id,omitempty"`
		Fence       uint64 `json:"fence,omitempty"`
		LeaseTTLMS  int64  `json:"lease_ttl_ms,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	ownerID, fence := taskOwnerAndFence(r, req.SessionID, req.Fence)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.tasks.Renew(ctx, workspaceID, id, ownerID, fence, time.Duration(req.LeaseTTLMS)*time.Millisecond, requestActor(r).ID)
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *server) handleTaskFail(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	var req struct {
		WorkspaceID    string `json:"workspace_id,omitempty"`
		SessionID      string `json:"session_id,omitempty"`
		Fence          uint64 `json:"fence,omitempty"`
		Error          string `json:"error"`
		FailureClass   string `json:"failure_class,omitempty"`
		Retryable      *bool  `json:"retryable,omitempty"`
		RetryAfterMS   *int64 `json:"retry_after_ms,omitempty"`
		IdempotencyKey string `json:"idempotency_key,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	ownerID, fence := taskOwnerAndFence(r, req.SessionID, req.Fence)
	workspaceID := requestWorkspace(r, req.WorkspaceID)
	var retryAfter *time.Duration
	if req.RetryAfterMS != nil {
		delay := time.Duration(*req.RetryAfterMS) * time.Millisecond
		retryAfter = &delay
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.tasks.Fail(ctx, store.TaskFailureInput{WorkspaceID: workspaceID, TaskID: id, OwnerID: ownerID, Fence: fence, Error: req.Error, FailureClass: req.FailureClass, Retryable: req.Retryable, RetryAfter: retryAfter, IdempotencyKey: req.IdempotencyKey, ActorID: requestActor(r).ID, Payload: body})
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeTaskMutationResponse(w, id, result)
}

func (s *server) handleTaskCancel(w http.ResponseWriter, r *http.Request, id int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	var req struct {
		WorkspaceID    string `json:"workspace_id,omitempty"`
		SessionID      string `json:"session_id,omitempty"`
		Fence          uint64 `json:"fence,omitempty"`
		Reason         string `json:"reason,omitempty"`
		IdempotencyKey string `json:"idempotency_key,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	ownerID, fence := taskOwnerAndFence(r, req.SessionID, req.Fence)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.tasks.Cancel(ctx, store.TaskCancelInput{WorkspaceID: requestWorkspace(r, req.WorkspaceID), TaskID: id, OwnerID: ownerID, Fence: fence, Reason: req.Reason, IdempotencyKey: req.IdempotencyKey, ActorID: requestActor(r).ID, Payload: body})
	if err != nil {
		writeTaskStoreErr(w, err)
		return
	}
	writeTaskMutationResponse(w, id, result)
}

func writeTaskMutationResponse(w http.ResponseWriter, id int64, result store.TaskMutationResult) {
	writeJSON(w, 200, map[string]any{"id": id, "status": result.Task.Status, "event_id": result.Event.EventID, "event_sequence": result.Event.Sequence, "deduped": result.Deduped, "retry_scheduled": result.RetryScheduled})
}

func requestWorkspace(r *http.Request, bodyWorkspace string) string {
	if principal, ok := principalFromRequest(r); ok && !principal.Development {
		return principal.WorkspaceID
	}
	workspaceID := strings.TrimSpace(bodyWorkspace)
	if workspaceID == "" {
		workspaceID = strings.TrimSpace(r.Header.Get("X-Workspace-ID"))
	}
	if workspaceID == "" {
		workspaceID = contracts.DefaultWorkspaceID
	}
	return workspaceID
}

func taskOwnerAndFence(r *http.Request, owner string, fence uint64) (string, uint64) {
	if strings.TrimSpace(owner) == "" {
		owner = strings.TrimSpace(r.Header.Get("X-Session-ID"))
	}
	if fence == 0 {
		if raw := strings.TrimSpace(r.Header.Get("X-Task-Fence")); raw != "" {
			fence, _ = strconv.ParseUint(raw, 10, 64)
		}
	}
	return strings.TrimSpace(owner), fence
}

func boolPtr(value bool) *bool { return &value }

func writeTaskStoreErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, store.ErrTaskNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrTaskNoReady):
		status = http.StatusNoContent
	case errors.Is(err, store.ErrTaskLeaseExpired), errors.Is(err, store.ErrTaskLeaseReleased):
		status = http.StatusGone
	case errors.Is(err, store.ErrTaskLeaseFenced), errors.Is(err, store.ErrTaskLeaseOwned), errors.Is(err, store.ErrTaskLeaseHeld),
		errors.Is(err, store.ErrTaskNotClaimed), errors.Is(err, store.ErrTaskTerminal), errors.Is(err, store.ErrTaskSessionBusy),
		errors.Is(err, store.ErrTaskDependencyMissing), errors.Is(err, store.ErrTaskDependencyCycle), errors.Is(err, store.ErrTaskRetryBudgetExhausted),
		errors.Is(err, store.ErrIdempotencyConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrTaskLeaseMissing):
		status = http.StatusPreconditionFailed
	case errors.Is(err, store.ErrTaskInvalidStatus):
		status = http.StatusBadRequest
	}
	writeErr(w, status, err.Error())
}

// ---------- v0.7 federation handlers ----------

func (s *server) handleFederationCoordSince(w http.ResponseWriter, r *http.Request, sinceID int64) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	host, _ := os.Hostname()
	rows, err := s.pool.Query(ctx, `
		SELECT id, sender, recipient, subject, body, COALESCE(host,''), COALESCE(origin_host, $1), ts
		FROM public.coord_messages
		WHERE id > $2 AND COALESCE(origin_host, $1) = $1
		ORDER BY id ASC LIMIT 500`, host, sinceID)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	msgs := []coordMsg{}
	for rows.Next() {
		var m coordMsg
		if err := rows.Scan(&m.ID, &m.Sender, &m.Recipient, &m.Subject, &m.Body, &m.Host, &m.OriginHost, &m.TS); err != nil {
			continue
		}
		msgs = append(msgs, m)
	}
	writeJSON(w, 200, map[string]any{"messages": msgs, "count": len(msgs), "origin_host": host})
}

func (s *server) handleFederationCoordImport(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var msgs []federationImportMsg
	if err := json.NewDecoder(r.Body).Decode(&msgs); err != nil {
		writeErr(w, 400, "invalid json: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	host, _ := os.Hostname()
	imported := 0
	for _, m := range msgs {
		if m.OriginHost == "" || m.OriginHost == host {
			// Skip messages with no origin marker or that originated here — never re-import our own.
			continue
		}
		ts := m.TS
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		_, err := s.pool.Exec(ctx, `
			INSERT INTO public.coord_messages (sender, recipient, subject, body, host, origin_host, ts)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			m.Sender, m.Recipient, m.Subject, m.Body, m.OriginHost, m.OriginHost, ts)
		if err != nil {
			log.Printf("federation import warn: %v", err)
			continue
		}
		imported++
	}
	writeJSON(w, 200, map[string]any{"imported": imported, "received": len(msgs)})
}

func (s *server) handleFederationPeerUpsert(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req federationPeerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.ID == "" || req.URL == "" || req.BearerToken == "" {
		writeErr(w, 400, "id, url, bearer_token required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO fornix.federation_peers (id, url, bearer_token)
		VALUES ($1,$2,$3)
		ON CONFLICT (id) DO UPDATE SET url=EXCLUDED.url, bearer_token=EXCLUDED.bearer_token`,
		req.ID, req.URL, req.BearerToken)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"id": req.ID, "registered": true})
}

func (s *server) handleFederationPeersList(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id, url, last_pull_at, last_pull_high_water FROM fornix.federation_peers ORDER BY id`)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	out := []federationPeerRow{}
	for rows.Next() {
		var p federationPeerRow
		var lpa *time.Time
		if err := rows.Scan(&p.ID, &p.URL, &lpa, &p.LastPullHighWater); err != nil {
			continue
		}
		p.LastPullAt = lpa
		out = append(out, p)
	}
	writeJSON(w, 200, map[string]any{"peers": out, "count": len(out)})
}

// ---------- v0.8 router learning handlers ----------

func (s *server) handleRouterObservation(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	var req routerObservationReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid json")
		return
	}
	if req.RequestHash == "" || req.TaskCategory == "" || req.ModelID == "" {
		writeErr(w, 400, "request_hash, task_category, model_id required")
		return
	}
	if req.Outcome == "" {
		req.Outcome = "unknown"
	}

	// v0.9 D4: if caller did not supply outcome_score, grade it automatically
	// so /v1/router/recommend ranks more sharply on partial telemetry.
	var graded *float64
	if req.OutcomeScore == nil {
		score := gradeOutcome(req)
		graded = &score
	} else {
		graded = req.OutcomeScore
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO fornix.router_observations (request_hash, task_category, model_id, cost_usd, latency_ms, outcome, outcome_score)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING id`,
		req.RequestHash, req.TaskCategory, req.ModelID, req.CostUSD, req.LatencyMs, req.Outcome, graded,
	).Scan(&id)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "recorded": true, "outcome_score": graded, "graded": req.OutcomeScore == nil})
}

// gradeOutcome assigns a 0..1 quality score from coarse signals on the
// router observation. Conservative — explicit caller scores always override.
func gradeOutcome(req routerObservationReq) float64 {
	outcome := strings.ToLower(strings.TrimSpace(req.Outcome))
	switch outcome {
	case "failed", "error", "timeout":
		return 0.0
	case "partial":
		return 0.4
	}
	score := 0.9
	if outcome == "unknown" {
		score = 0.6
	}
	if req.LatencyMs > 60_000 {
		score -= 0.2
	}
	if req.CostUSD < 0 {
		score -= 0.3
	}
	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}
	return score
}

func (s *server) handleRouterRecommend(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, 401, "unauthorised")
		return
	}
	category := r.URL.Query().Get("category")
	if category == "" {
		writeErr(w, 400, "category required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT model_id,
		       AVG(cost_usd)::float8 AS cost_usd_avg,
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::float8 AS latency_p50,
		       (SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END)::float8 / NULLIF(COUNT(*),0))::float8 AS success_rate,
		       COUNT(*)::int AS sample_size
		FROM fornix.router_observations
		WHERE task_category = $1 AND observed_at > now() - INTERVAL '30 days'
		GROUP BY model_id
		HAVING COUNT(*) >= 1`, category)
	if err != nil {
		writeErr(w, 500, "db: "+err.Error())
		return
	}
	defer rows.Close()
	out := []routerRecommendation{}
	for rows.Next() {
		var rec routerRecommendation
		if err := rows.Scan(&rec.ModelID, &rec.CostUSDAvg, &rec.LatencyP50, &rec.SuccessRate, &rec.SampleSize); err != nil {
			continue
		}
		out = append(out, rec)
	}
	// Sort by (success_rate / cost_usd_avg) DESC — cheapest model meeting quality wins.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			si := out[i].SuccessRate / max(out[i].CostUSDAvg, 1e-9)
			sj := out[j].SuccessRate / max(out[j].CostUSDAvg, 1e-9)
			if sj > si {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	writeJSON(w, 200, map[string]any{"category": category, "recommendations": out})
}

// ---------- background workers ----------

// sessionsReaper marks sessions whose heartbeat is older than 90s as offline.
// Task recovery is governed by task_execution_leases and fencing; directly
// rewriting a claimed task here would bypass the authoritative lease epoch.
func (s *server) sessionsReaper(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rc, cancel := context.WithTimeout(ctx, 10*time.Second)
			if _, err := s.pool.Exec(rc, `
				UPDATE fornix.sessions
				SET status='offline', current_task_id=NULL
				WHERE last_heartbeat < now() - INTERVAL '90 seconds' AND status <> 'offline'`); err != nil {
				log.Printf("reaper: sessions update warn: %v", err)
			}
			cancel()
		}
	}
}

// federationPoller polls each registered peer's /v1/federation/coord/since/:high_water every 5s
// and bulk-imports anything new.
func (s *server) federationPoller(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.pollPeersOnce(ctx)
		}
	}
}

func (s *server) pollPeersOnce(ctx context.Context) {
	rc, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.pool.Query(rc, `SELECT id, url, bearer_token, last_pull_high_water FROM fornix.federation_peers`)
	if err != nil {
		log.Printf("federation poll: list peers warn: %v", err)
		return
	}
	type peer struct {
		id        string
		url       string
		token     string
		highWater int64
	}
	var peers []peer
	for rows.Next() {
		var p peer
		if err := rows.Scan(&p.id, &p.url, &p.token, &p.highWater); err != nil {
			continue
		}
		peers = append(peers, p)
	}
	rows.Close()

	for _, p := range peers {
		s.pullFromPeer(ctx, p.id, p.url, p.token, p.highWater)
	}
}

func (s *server) pullFromPeer(ctx context.Context, peerID, peerURL, token string, highWater int64) {
	endpoint := strings.TrimRight(peerURL, "/") + "/v1/federation/coord/since/" + strconv.FormatInt(highWater, 10)
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("federation poll %s: %v", peerID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("federation poll %s: status %d: %s", peerID, resp.StatusCode, string(body))
		return
	}
	var payload struct {
		Messages   []coordMsg `json:"messages"`
		OriginHost string     `json:"origin_host"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		log.Printf("federation poll %s: decode: %v", peerID, err)
		return
	}
	if len(payload.Messages) == 0 {
		return
	}
	host, _ := os.Hostname()
	var newHigh int64 = highWater
	imported := 0
	for _, m := range payload.Messages {
		origin := m.OriginHost
		if origin == "" {
			origin = payload.OriginHost
		}
		if origin == "" || origin == host {
			continue
		}
		_, err := s.pool.Exec(ctx, `
			INSERT INTO public.coord_messages (sender, recipient, subject, body, host, origin_host, ts)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			m.Sender, m.Recipient, m.Subject, m.Body, origin, origin, m.TS)
		if err != nil {
			log.Printf("federation import %s id=%d warn: %v", peerID, m.ID, err)
			continue
		}
		imported++
		if m.ID > newHigh {
			newHigh = m.ID
		}
	}
	if newHigh > highWater {
		if _, err := s.pool.Exec(ctx, `
			UPDATE fornix.federation_peers SET last_pull_at=now(), last_pull_high_water=$1 WHERE id=$2`,
			newHigh, peerID); err != nil {
			log.Printf("federation poll %s: high-water update warn: %v", peerID, err)
		}
		log.Printf("federation pull %s: imported=%d new_high_water=%d", peerID, imported, newHigh)
	}
}

// ---------- routing ----------

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleLiveness)
	mux.HandleFunc("/readyz", s.handleReadiness)
	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/operator/workspaces/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleOperatorBootstrap(w, r)
	})
	mux.HandleFunc("/v1/operator/workspaces/", s.handleOperatorWorkspaces)
	mux.HandleFunc("/v1/operator/workspaces", s.handleOperatorWorkspaces)
	mux.HandleFunc("/v1/operator/identities/", s.handleOperatorIdentities)
	mux.HandleFunc("/v1/operator/identities", s.handleOperatorIdentities)
	mux.HandleFunc("/v1/operator/roles/", s.handleOperatorRoles)
	mux.HandleFunc("/v1/operator/roles", s.handleOperatorRoles)
	mux.HandleFunc("/v1/operator/api-keys/", s.handleOperatorAPIKeys)
	mux.HandleFunc("/v1/operator/api-keys", s.handleOperatorAPIKeys)
	mux.HandleFunc("/v1/operator/ingests/", s.handleOperatorIngest)
	mux.HandleFunc("/v1/operator/ingests", s.handleOperatorIngests)
	mux.HandleFunc("/v1/operator/ingest/dry-run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleIngestDryRun(w, r)
	})
	mux.HandleFunc("/v1/operator/ingest/jobs/", s.handleIngestJob)
	mux.HandleFunc("/v1/operator/ingest/jobs", s.handleIngestJobs)
	mux.HandleFunc("/v1/observability/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		s.handleObservabilityMetrics(w, r)
	})
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		s.handleObservabilityMetrics(w, r)
	})
	mux.HandleFunc("/v1/model/complete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleModelComplete(w, r)
	})
	mux.HandleFunc("/v1/tools/execute", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleToolExecute(w, r)
	})
	mux.HandleFunc("/v1/tools/approvals/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/decide") {
			writeErr(w, http.StatusMethodNotAllowed, "POST /decide only")
			return
		}
		s.handleToolApprovalDecision(w, r)
	})
	mux.HandleFunc("/v1/agent/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleAgentRunCreate(w, r)
	})
	mux.HandleFunc("/v1/agent/run/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/agent/run/")
		parts := strings.SplitN(rest, "/", 2)
		if strings.TrimSpace(parts[0]) == "" {
			writeErr(w, http.StatusNotFound, "agent run id required")
			return
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			s.handleAgentRunGet(w, r, parts[0])
			return
		}
		if len(parts) != 2 || r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST /advance, /cancel, or /replay only")
			return
		}
		switch parts[1] {
		case "advance":
			s.handleAgentRunAdvance(w, r, parts[0])
		case "cancel":
			s.handleAgentRunCancel(w, r, parts[0])
		case "external/wait":
			s.handleAgentRunExternal(w, r, parts[0], "wait")
		case "external/complete":
			s.handleAgentRunExternal(w, r, parts[0], "complete")
		case "replay":
			s.handleAgentRunReplay(w, r, parts[0])
		default:
			writeErr(w, http.StatusNotFound, "unknown agent run operation")
		}
	})
	mux.HandleFunc("/v1/retrieve", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleRetrieve(w, r)
	})
	mux.HandleFunc("/v1/evaluations/datasets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleEvaluationDatasetCreate(w, r)
	})
	mux.HandleFunc("/v1/evaluations/retrieval/surfaces", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleRetrievalSurfaceList(w, r)
		case http.MethodPost:
			s.handleRetrievalSurfaceRegister(w, r)
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
		}
	})
	mux.HandleFunc("/v1/evaluations/retrieval/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleRetrievalEvaluationRun(w, r)
	})
	mux.HandleFunc("/v1/evaluations/runs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/v1/evaluations/runs/")
		if id == "" || strings.Contains(id, "/") {
			writeErr(w, http.StatusNotFound, "evaluation run id required")
			return
		}
		s.handleEvaluationRunGet(w, r, id)
	})
	mux.HandleFunc("/v1/evidence", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleEvidencePut(w, r)
	})
	mux.HandleFunc("/v1/evidence/edge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleEvidenceEdge(w, r)
	})
	mux.HandleFunc("/v1/evidence/disclose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleEvidenceDisclosure(w, r)
	})
	mux.HandleFunc("/v1/evidence/provenance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleEvidenceProvenance(w, r)
	})
	mux.HandleFunc("/v1/artifacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactPut(w, r)
	})
	mux.HandleFunc("/v1/artifacts/disclose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactDisclosure(w, r)
	})
	mux.HandleFunc("/v1/artifacts/provenance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactProvenance(w, r)
	})
	mux.HandleFunc("/v1/artifacts/backfill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactBackfill(w, r)
	})
	mux.HandleFunc("/v1/artifacts/retention", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactRetention(w, r)
	})
	mux.HandleFunc("/v1/artifacts/integrity", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleArtifactIntegrity(w, r)
	})
	mux.HandleFunc("/v1/artifacts/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		s.handleArtifactMetrics(w, r)
	})
	mux.HandleFunc("/v1/memo/search", s.handleSearch)
	mux.HandleFunc("/v1/memo/backfill", s.handleBackfillEmbeddings)
	mux.HandleFunc("/v1/memo/", func(w http.ResponseWriter, r *http.Request) {
		// /v1/memo/:id with GET/PUT/DELETE
		idStr := strings.TrimPrefix(r.URL.Path, "/v1/memo/")
		if idStr == "" {
			writeErr(w, 404, "memo id required")
			return
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			writeErr(w, 400, "bad id")
			return
		}
		switch r.Method {
		case "GET":
			s.handleGet(w, r, id)
		case "PUT":
			s.handleUpdate(w, r, id)
		case "DELETE":
			s.handleDelete(w, r, id)
		default:
			writeErr(w, 405, "method not allowed")
		}
	})
	mux.HandleFunc("/v1/memo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleCreate(w, r)
	})
	mux.HandleFunc("/v1/coord", s.handleCoordSend)
	mux.HandleFunc("/v1/coord/recent", s.handleCoordRecent)

	// ---------- v0.5 code graph ----------
	mux.HandleFunc("/v1/symbol/search", s.handleSymbolSearch)
	mux.HandleFunc("/v1/symbol/edge", s.handleSymbolEdge)
	mux.HandleFunc("/v1/symbol/reindex", s.handleSymbolReindex)
	mux.HandleFunc("/v1/symbol/", func(w http.ResponseWriter, r *http.Request) {
		// /v1/symbol/:id/callers | /v1/symbol/:id/callees
		rest := strings.TrimPrefix(r.URL.Path, "/v1/symbol/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 && (parts[1] == "callers" || parts[1] == "callees") {
			id, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				writeErr(w, 400, "bad id")
				return
			}
			if r.Method != "GET" {
				writeErr(w, 405, "GET only")
				return
			}
			s.handleSymbolNeighbours(w, r, id, parts[1])
			return
		}
		writeErr(w, 404, "unknown symbol route")
	})
	mux.HandleFunc("/v1/symbol", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleSymbolUpsert(w, r)
	})

	// ---------- v0.6 orchestrator ----------
	mux.HandleFunc("/v1/session", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleSessionRegister(w, r)
	})
	mux.HandleFunc("/v1/sessions", s.handleSessionsList)
	mux.HandleFunc("/v1/session/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/session/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 && parts[1] == "heartbeat" {
			if r.Method != "POST" {
				writeErr(w, 405, "POST only")
				return
			}
			s.handleSessionHeartbeat(w, r, parts[0])
			return
		}
		writeErr(w, 404, "unknown session route")
	})
	mux.HandleFunc("/v1/task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleTaskCreate(w, r)
	})
	mux.HandleFunc("/v1/tasks", s.handleTasksList)
	mux.HandleFunc("/v1/task/claim", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleTaskClaim(w, r)
	})
	mux.HandleFunc("/v1/task/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/task/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 1 && r.Method == http.MethodGet {
			id, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, "bad id")
				return
			}
			s.handleTaskGet(w, r, id)
			return
		}
		if len(parts) == 2 && (parts[1] == "complete" || parts[1] == "renew" || parts[1] == "fail" || parts[1] == "cancel") {
			id, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				writeErr(w, 400, "bad id")
				return
			}
			if r.Method != "POST" {
				writeErr(w, 405, "POST only")
				return
			}
			switch parts[1] {
			case "complete":
				s.handleTaskComplete(w, r, id)
			case "renew":
				s.handleTaskRenew(w, r, id)
			case "fail":
				s.handleTaskFail(w, r, id)
			case "cancel":
				s.handleTaskCancel(w, r, id)
			}
			return
		}
		writeErr(w, 404, "unknown task route")
	})

	// ---------- v0.7 federation ----------
	mux.HandleFunc("/v1/federation/coord/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleFederationCoordImport(w, r)
	})
	mux.HandleFunc("/v1/federation/coord/since/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writeErr(w, 405, "GET only")
			return
		}
		idStr := strings.TrimPrefix(r.URL.Path, "/v1/federation/coord/since/")
		sinceID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			writeErr(w, 400, "bad since id")
			return
		}
		s.handleFederationCoordSince(w, r, sinceID)
	})
	mux.HandleFunc("/v1/federation/peer", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleFederationPeerUpsert(w, r)
	})
	mux.HandleFunc("/v1/federation/peers", s.handleFederationPeersList)

	// ---------- v0.8 router learning ----------
	mux.HandleFunc("/v1/router/observation", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleRouterObservation(w, r)
	})
	mux.HandleFunc("/v1/router/recommend", s.handleRouterRecommend)

	// ---------- v0.10 RAG (Router <-> Fornix integration) ----------
	mux.HandleFunc("/v1/rag", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleRAG(w, r)
	})
	mux.HandleFunc("/v1/chunks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeErr(w, 405, "POST only")
			return
		}
		s.handleChunkUpsert(w, r)
	})

	return mux
}
