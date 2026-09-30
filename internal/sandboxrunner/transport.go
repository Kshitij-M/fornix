package sandboxrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	runnerRunPath       = "/v1/attempts/run"
	runnerReconcilePath = "/v1/attempts/reconcile"
	runnerCleanupPath   = "/v1/attempts/cleanup"
	runnerHealthPath    = "/v1/health"
	maxUnixSocketPath   = 103
	maxRunnerConcurrent = 4
)

var (
	// ErrRunnerUnavailable is returned when the authenticated local runner
	// cannot complete an IPC operation. Raw transport and runtime errors are
	// intentionally not included in the message.
	ErrRunnerUnavailable = errors.New("sandbox runner is unavailable")
	// ErrRunnerIPCAuth reports an absent or invalid local IPC credential.
	ErrRunnerIPCAuth = errors.New("sandbox runner IPC authentication failed")
	// ErrRunnerIPCRequest reports a request rejected before runtime execution.
	ErrRunnerIPCRequest = errors.New("sandbox runner IPC request is invalid")
	// ErrRunnerIPCBusy reports that all bounded runner execution slots are busy.
	ErrRunnerIPCBusy = errors.New("sandbox runner is at its concurrency limit")
)

// Runtime owns deterministic create/start/inspect/wait/log/reconcile/cleanup
// operations against one local sandbox Engine. Implementations must never
// relaunch an attempt during reconciliation and must bind runtime objects to
// both the execution identity and request hash.
type Runtime interface {
	RunAttempt(context.Context, contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error)
	ReconcileAttempt(context.Context, contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error)
	CleanupAttempt(context.Context, contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error)
}

// ReconcileRequest is the bounded wire payload for inspecting one exact
// durable execution attempt. It contains no command, path, or secret.
type ReconcileRequest struct {
	SchemaVersion int                                `json:"schema_version"`
	Execution     contracts.SandboxExecutionIdentity `json:"execution"`
}

// Normalize validates the execution identity and protocol version.
func (r *ReconcileRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("%w: reconciliation request is nil", ErrRunnerIPCRequest)
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = contracts.SandboxRunnerProtocolVersion
	}
	if r.SchemaVersion != contracts.SandboxRunnerProtocolVersion {
		return fmt.Errorf("%w: unsupported reconciliation version", ErrRunnerIPCRequest)
	}
	if err := r.Execution.Normalize(); err != nil {
		return fmt.Errorf("%w: invalid execution identity", ErrRunnerIPCRequest)
	}
	return nil
}

// Handler serves the private runner protocol. It must only be attached to a
// Unix-domain listener created by Listen; mounting the socket is an explicit
// local-runtime action. The bearer token authenticates callers but does not
// replace workspace, definition, profile, effect, or fencing checks.
type Handler struct {
	runtime    Runtime
	tokenHash  [sha256.Size]byte
	semaphore  chan struct{}
	requestMax int64
}

// NewHandler constructs a fail-closed handler using a random 256-bit
// lowercase-hex credential generated and stored by the local runtime manager.
// The raw token is retained only as a one-way digest.
func NewHandler(runtime Runtime, token string) (*Handler, error) {
	if runtime == nil {
		return nil, fmt.Errorf("sandbox runner runtime is required")
	}
	if len(token) != sha256.Size*2 {
		return nil, ErrRunnerIPCAuth
	}
	decoded, err := hex.DecodeString(token)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(token) != token {
		return nil, ErrRunnerIPCAuth
	}
	return &Handler{
		runtime: runtime, tokenHash: sha256.Sum256(decoded),
		semaphore: make(chan struct{}, maxRunnerConcurrent), requestMax: contracts.MaxSandboxRunnerRequestBytes,
	}, nil
}

// ServeHTTP routes the versioned local protocol and never emits command,
// environment, host-path, credential, or raw runtime-error details.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.runtime == nil || !h.authorized(r) {
		writeRunnerError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.URL.RawQuery != "" || r.URL.Fragment != "" {
		writeRunnerError(w, http.StatusNotFound, "not_found")
		return
	}
	switch {
	case r.URL.Path == runnerHealthPath && r.Method == http.MethodGet:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == runnerRunPath && r.Method == http.MethodPost:
		h.run(w, r)
	case r.URL.Path == runnerReconcilePath && r.Method == http.MethodPost:
		h.reconcile(w, r)
	case r.URL.Path == runnerCleanupPath && r.Method == http.MethodPost:
		h.cleanup(w, r)
	default:
		writeRunnerError(w, http.StatusNotFound, "not_found")
	}
}

func (h *Handler) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	digest := sha256.Sum256(provided)
	return subtle.ConstantTimeCompare(digest[:], h.tokenHash[:]) == 1
}

func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	var request contracts.SandboxRunnerRequest
	if err := decodeRunnerJSON(w, r, h.requestMax, &request); err != nil || request.Normalize() != nil {
		writeRunnerError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !h.acquire(w) {
		return
	}
	defer h.release()
	allowance := time.Duration(request.Profile.TimeoutMS)*time.Millisecond + 30*time.Second
	ctx, cancel := context.WithTimeout(r.Context(), allowance)
	defer cancel()
	response, err := h.runtime.RunAttempt(ctx, request)
	if err != nil {
		writeRunnerError(w, http.StatusServiceUnavailable, "attempt_uncertain")
		return
	}
	body, err := response.MarshalBoundedFor(request)
	if err != nil || int64(len(body)) > int64(contracts.MaxSandboxRunnerResponseBytes) {
		writeRunnerError(w, http.StatusBadGateway, "invalid_runtime_response")
		return
	}
	writeRunnerJSON(w, http.StatusOK, body)
}

func (h *Handler) reconcile(w http.ResponseWriter, r *http.Request) {
	var request ReconcileRequest
	if err := decodeRunnerJSON(w, r, 16<<10, &request); err != nil || request.Normalize() != nil {
		writeRunnerError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !h.acquire(w) {
		return
	}
	defer h.release()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	observation, err := h.runtime.ReconcileAttempt(ctx, request.Execution)
	if err != nil || observation.Normalize(request.Execution) != nil {
		writeRunnerError(w, http.StatusServiceUnavailable, "reconciliation_unknown")
		return
	}
	body, err := json.Marshal(observation)
	if err != nil || len(body) > contracts.MaxSandboxRunnerResponseBytes {
		writeRunnerError(w, http.StatusBadGateway, "invalid_runtime_response")
		return
	}
	writeRunnerJSON(w, http.StatusOK, body)
}

func (h *Handler) cleanup(w http.ResponseWriter, r *http.Request) {
	var command contracts.SandboxCleanupCommand
	if err := decodeRunnerJSON(w, r, 32<<10, &command); err != nil || command.Normalize() != nil {
		writeRunnerError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !h.acquire(w) {
		return
	}
	defer h.release()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	observation, err := h.runtime.CleanupAttempt(ctx, command)
	if err != nil || observation.NormalizeForCommand(command) != nil {
		writeRunnerError(w, http.StatusServiceUnavailable, "cleanup_unknown")
		return
	}
	body, err := json.Marshal(observation)
	if err != nil || len(body) > contracts.MaxSandboxRunnerResponseBytes {
		writeRunnerError(w, http.StatusBadGateway, "invalid_runtime_response")
		return
	}
	writeRunnerJSON(w, http.StatusOK, body)
}

func (h *Handler) acquire(w http.ResponseWriter) bool {
	select {
	case h.semaphore <- struct{}{}:
		return true
	default:
		writeRunnerError(w, http.StatusServiceUnavailable, "runner_busy")
		return false
	}
}

func (h *Handler) release() { <-h.semaphore }

func decodeRunnerJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, target any) error {
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if r.ContentLength > maxBytes || mediaErr != nil || mediaType != "application/json" {
		return ErrRunnerIPCRequest
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrRunnerIPCRequest
	}
	if decoder.Decode(new(any)) != io.EOF {
		return ErrRunnerIPCRequest
	}
	return nil
}

func writeRunnerError(w http.ResponseWriter, status int, code string) {
	writeRunnerJSON(w, status, []byte(`{"error":"`+code+`"}`))
}

func writeRunnerJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// Listen creates a private local socket and serves the authenticated runner
// protocol until the server is shut down. It creates a missing parent with
// mode 0700 and rejects an existing parent unless it is private and
// owner-accessible; it never changes permissions on an existing directory.
// The socket is world-connectable because container UIDs vary across supported
// hosts, so authentication is mandatory even for local callers.
func Listen(ctx context.Context, socketPath, token string, runtime Runtime) error {
	if ctx == nil {
		return ErrRunnerIPCRequest
	}
	handler, err := NewHandler(runtime, token)
	if err != nil {
		return err
	}
	listener, err := listenUnix(socketPath)
	if err != nil {
		return ErrRunnerUnavailable
	}
	return serveListener(ctx, listener, handler)
}

// serveListener owns request admission and shutdown. When the parent context
// is cancelled it rejects new work, cancels in-flight runtime calls, closes
// the listener, and waits for a bounded drain interval. If a runtime ignores
// cancellation, shutdown returns an error rather than hanging; that interrupted
// attempt must be recovered from durable identity and is never retried here.
func serveListener(ctx context.Context, listener net.Listener, handler http.Handler) error {
	return serveListenerWithTimeout(ctx, listener, handler, 5*time.Second)
}

func serveListenerWithTimeout(ctx context.Context, listener net.Listener, handler http.Handler, shutdownTimeout time.Duration) error {
	if ctx == nil || listener == nil || handler == nil {
		return ErrRunnerIPCRequest
	}
	defer listener.Close()
	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	defer cancelRunner()
	var lifecycleMu sync.Mutex
	stopping := false
	var active sync.WaitGroup
	guardedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lifecycleMu.Lock()
		if stopping {
			lifecycleMu.Unlock()
			writeRunnerError(w, http.StatusServiceUnavailable, "runner_stopping")
			return
		}
		active.Add(1)
		lifecycleMu.Unlock()
		defer active.Done()

		requestCtx, cancelRequest := context.WithCancel(r.Context())
		stopCancel := context.AfterFunc(runnerCtx, cancelRequest)
		defer func() {
			stopCancel()
			cancelRequest()
		}()
		handler.ServeHTTP(w, r.WithContext(requestCtx))
	})
	server := &http.Server{
		Handler: guardedHandler, ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		ErrorLog: log.New(io.Discard, "", 0),
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	serveErr := error(nil)
	select {
	case serveErr = <-serveDone:
	case <-ctx.Done():
	}
	if ctx.Err() == nil {
		// Serve exited independently of the manager's shutdown request. Cancel
		// every request and fail closed instead of reporting a healthy stop.
		beginStopping := func() {
			lifecycleMu.Lock()
			stopping = true
			lifecycleMu.Unlock()
			cancelRunner()
		}
		beginStopping()
		_ = server.Close()
		_ = waitForHandlers(&active, shutdownTimeout)
		return ErrRunnerUnavailable
	}

	lifecycleMu.Lock()
	stopping = true
	lifecycleMu.Unlock()
	cancelRunner()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil {
		_ = server.Close()
	}
	if serveErr == nil {
		serveErr = <-serveDone
	}
	drained := waitForHandlers(&active, shutdownTimeout)
	if shutdownErr != nil || (serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed)) {
		return ErrRunnerUnavailable
	}
	if !drained {
		return ErrRunnerUnavailable
	}
	return nil
}

func waitForHandlers(active *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		active.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func listenUnix(socketPath string) (*net.UnixListener, error) {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || len(socketPath) > maxUnixSocketPath {
		return nil, fmt.Errorf("invalid sandbox runner socket path")
	}
	parent := filepath.Dir(socketPath)
	if err := ensureSocketDirectory(parent); err != nil {
		return nil, fmt.Errorf("prepare sandbox runner socket directory")
	}
	canonicalParent, err := filepath.EvalSymlinks(parent)
	if err != nil || canonicalParent != parent {
		return nil, fmt.Errorf("sandbox runner socket directory must not use symlink aliases")
	}
	if err := validateSocketDirectory(parent); err != nil {
		return nil, err
	}
	if err := removeStaleSocket(socketPath); err != nil {
		return nil, err
	}
	address, err := net.ResolveUnixAddr("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("invalid sandbox runner socket path")
	}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("listen on sandbox runner socket: %w", err)
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(socketPath, 0o666); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("secure sandbox runner socket")
	}
	return listener, nil
}

func removeStaleSocket(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("sandbox runner socket path is occupied")
	}
	conn, dialErr := net.DialTimeout("unix", socketPath, 250*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("sandbox runner socket is already active")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("sandbox runner socket state cannot be verified")
	}
	current, statErr := os.Lstat(socketPath)
	if statErr != nil || !os.SameFile(info, current) || current.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("sandbox runner socket changed during stale-socket check")
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove stale sandbox runner socket")
	}
	return nil
}

// Client sends bounded runner protocol calls through one Unix-domain socket.
// Its custom dialer ignores HTTP URL hosts; no TCP fallback exists.
type Client struct {
	client    *http.Client
	socket    string
	token     string
	transport *http.Transport
}

// NewClient constructs an authenticated Unix-socket client. The token must
// match the runner's locally generated 256-bit credential.
func NewClient(socketPath, token string) (*Client, error) {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || len(socketPath) > maxUnixSocketPath {
		return nil, fmt.Errorf("invalid sandbox runner socket path")
	}
	if len(token) != sha256.Size*2 {
		return nil, ErrRunnerIPCAuth
	}
	decoded, err := hex.DecodeString(token)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(token) != token {
		return nil, ErrRunnerIPCAuth
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: 5 * time.Second}
			return dialer.DialContext(ctx, "unix", socketPath)
		},
		MaxIdleConns: 16, MaxIdleConnsPerHost: 16, MaxConnsPerHost: maxRunnerConcurrent,
		IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 15 * time.Minute,
	}
	return &Client{
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		socket: socketPath, token: token, transport: transport,
	}, nil
}

// CloseIdleConnections releases cached Unix-socket connections.
func (c *Client) CloseIdleConnections() {
	if c != nil && c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

// Health performs the authenticated startup handshake. A failed handshake
// means the non-local backend must remain unavailable.
func (c *Client) Health(ctx context.Context) error {
	if c == nil || c.client == nil {
		return ErrRunnerUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fornix.runner"+runnerHealthPath, nil)
	if err != nil {
		return ErrRunnerUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.client.Do(request)
	if err != nil {
		return ErrRunnerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return ErrRunnerUnavailable
	}
	return nil
}

// RunAttempt submits one exact bounded request. It never retries a transport
// failure because execution may already have started; callers must reconcile.
func (c *Client) RunAttempt(ctx context.Context, request contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error) {
	var response contracts.SandboxRunnerResponse
	if err := request.Normalize(); err != nil {
		return response, ErrRunnerIPCRequest
	}
	body, err := request.MarshalBounded()
	if err != nil {
		return response, ErrRunnerIPCRequest
	}
	if err := c.post(ctx, runnerRunPath, body, contracts.MaxSandboxRunnerResponseBytes, &response); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if err := response.NormalizeFor(request); err != nil {
		return contracts.SandboxRunnerResponse{}, ErrRunnerUnavailable
	}
	return response, nil
}

// ReconcileAttempt inspects an existing exact attempt without invoking it.
func (c *Client) ReconcileAttempt(ctx context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	var observation contracts.SandboxAttemptObservation
	request := ReconcileRequest{SchemaVersion: contracts.SandboxRunnerProtocolVersion, Execution: identity}
	if err := request.Normalize(); err != nil {
		return observation, ErrRunnerIPCRequest
	}
	body, err := json.Marshal(request)
	if err != nil || len(body) > 16<<10 {
		return observation, ErrRunnerIPCRequest
	}
	if err := c.post(ctx, runnerReconcilePath, body, contracts.MaxSandboxRunnerResponseBytes, &observation); err != nil {
		return contracts.SandboxAttemptObservation{}, err
	}
	if err := observation.Normalize(identity); err != nil {
		return contracts.SandboxAttemptObservation{}, ErrRunnerUnavailable
	}
	return observation, nil
}

// CleanupAttempt inspects and removes only the exact object named by a
// durable cleanup intent. A transport failure is uncertain and is never
// retried by the client; the fenced Postgres worker owns retry scheduling.
func (c *Client) CleanupAttempt(ctx context.Context, command contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error) {
	var observation contracts.SandboxCleanupObservation
	if err := command.Normalize(); err != nil {
		return observation, ErrRunnerIPCRequest
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > 32<<10 {
		return observation, ErrRunnerIPCRequest
	}
	if err := c.post(ctx, runnerCleanupPath, body, contracts.MaxSandboxRunnerResponseBytes, &observation); err != nil {
		return contracts.SandboxCleanupObservation{}, err
	}
	if err := observation.NormalizeForCommand(command); err != nil {
		return contracts.SandboxCleanupObservation{}, ErrRunnerUnavailable
	}
	return observation, nil
}

func (c *Client) post(ctx context.Context, endpoint string, body []byte, responseMax int64, target any) error {
	if c == nil || c.client == nil || int64(len(body)) > contracts.MaxSandboxRunnerRequestBytes {
		return ErrRunnerUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://fornix.runner"+endpoint, bytes.NewReader(body))
	if err != nil {
		return ErrRunnerUnavailable
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.client.Do(request)
	if err != nil {
		return ErrRunnerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ErrRunnerUnavailable
	}
	if response.ContentLength > responseMax {
		return ErrRunnerUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, responseMax+1))
	if err != nil || int64(len(raw)) > responseMax {
		return ErrRunnerUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrRunnerUnavailable
	}
	if decoder.Decode(new(any)) != io.EOF {
		return ErrRunnerUnavailable
	}
	return nil
}
