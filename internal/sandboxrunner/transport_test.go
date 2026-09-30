package sandboxrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type fakeRuntime struct {
	mu             sync.Mutex
	runCalls       int
	reconcileCalls int
	cleanupCalls   int
	runErr         error
	cleanupErr     error
	badResponse    bool
	badCleanup     bool
}

type blockingRuntime struct {
	*fakeRuntime
	started chan struct{}
	release chan struct{}
}

type uninterruptibleRuntime struct {
	fakeRuntime *fakeRuntime
	started     chan struct{}
	release     chan struct{}
	returned    chan struct{}
}

func (r *uninterruptibleRuntime) RunAttempt(ctx context.Context, request contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error) {
	close(r.started)
	<-r.release
	close(r.returned)
	return r.fakeRuntime.RunAttempt(ctx, request)
}

func (r *uninterruptibleRuntime) ReconcileAttempt(ctx context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	return r.fakeRuntime.ReconcileAttempt(ctx, identity)
}

func (r *uninterruptibleRuntime) CleanupAttempt(ctx context.Context, command contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error) {
	return r.fakeRuntime.CleanupAttempt(ctx, command)
}

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

type pipeAddr string

func (pipeAddr) Network() string  { return "in-memory" }
func (a pipeAddr) String() string { return string(a) }

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn, 1), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*pipeListener) Addr() net.Addr { return pipeAddr("runner-test") }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func (r *blockingRuntime) RunAttempt(ctx context.Context, request contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error) {
	select {
	case r.started <- struct{}{}:
	case <-ctx.Done():
		return contracts.SandboxRunnerResponse{}, ctx.Err()
	}
	select {
	case <-r.release:
		return r.fakeRuntime.RunAttempt(ctx, request)
	case <-ctx.Done():
		return contracts.SandboxRunnerResponse{}, ctx.Err()
	}
}

func shortRunnerSocket(t *testing.T) string {
	t.Helper()
	tempRoot := "/tmp"
	if runtime.GOOS == "darwin" {
		tempRoot = "/private/tmp"
	}
	directory, err := os.MkdirTemp(tempRoot, "fxr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return filepath.Join(directory, "runner.sock")
}

func requireUnixSocket(t *testing.T) {
	t.Helper()
	listener, err := listenUnix(shortRunnerSocket(t))
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("execution environment disallows Unix-domain listeners: %v", err)
		}
		t.Fatalf("listen on temporary Unix socket: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close temporary Unix socket: %v", err)
	}
}

func (f *fakeRuntime) RunAttempt(_ context.Context, request contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error) {
	f.mu.Lock()
	f.runCalls++
	err, badResponse := f.runErr, f.badResponse
	f.mu.Unlock()
	if err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	response := contracts.SandboxRunnerResponse{
		SchemaVersion: contracts.SandboxRunnerProtocolVersion,
		RequestID:     request.RequestID,
		ExecutionHash: request.Execution.StableHash(),
		RequestHash:   request.StableHash(),
		Outcome:       contracts.SandboxRunnerOutcomeCompleted,
		Stdout:        []byte("bounded result"),
		StartedAt:     now,
		FinishedAt:    now.Add(time.Millisecond),
	}
	if badResponse {
		response.RequestHash = contracts.HashStrings("different request")
	}
	return response, nil
}

func (f *fakeRuntime) ReconcileAttempt(_ context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	f.mu.Lock()
	f.reconcileCalls++
	f.mu.Unlock()
	return contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptUnknown}, nil
}

func (f *fakeRuntime) CleanupAttempt(_ context.Context, command contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error) {
	f.mu.Lock()
	f.cleanupCalls++
	err, bad := f.cleanupErr, f.badCleanup
	f.mu.Unlock()
	if err != nil {
		return contracts.SandboxCleanupObservation{}, err
	}
	observation := contracts.SandboxCleanupObservation{
		WorkspaceID: command.Intent.WorkspaceID, JobID: command.JobID, OwnerID: command.OwnerID, Fence: command.Fence,
		IdentityHash: command.Intent.Identity.StableHash(), RequestHash: command.Intent.Identity.ToolRequestHash,
		State: contracts.SandboxCleanupAlreadyAbsent,
	}
	if bad {
		observation.IdentityHash = contracts.HashStrings("different cleanup identity")
	}
	return observation, nil
}

func (f *fakeRuntime) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runCalls, f.reconcileCalls
}

func (f *fakeRuntime) cleanupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanupCalls
}

func runnerCleanupCommand(t *testing.T) contracts.SandboxCleanupCommand {
	t.Helper()
	identity := runnerTestRequest(t).Execution
	intent := contracts.SandboxCleanupIntent{
		WorkspaceID: identity.WorkspaceID, ToolRunID: identity.ToolRunID, ToolAttempt: identity.ToolAttempt,
		DomainLinkID: "domain-link-a", Identity: identity, ResultHash: contracts.HashStrings("committed tool result"),
		Actor: contracts.ActorRef{ID: "cleanup-test-actor", Kind: "service", WorkspaceID: identity.WorkspaceID},
	}
	if err := intent.Normalize(); err != nil {
		t.Fatal(err)
	}
	command := contracts.SandboxCleanupCommand{
		SchemaVersion: contracts.SandboxCleanupProtocolVersion, JobID: "cleanup-job-a", OwnerID: "cleanup-worker-a", Fence: 3,
		IntentHash: intent.StableHash(), Intent: intent,
	}
	if err := command.Normalize(); err != nil {
		t.Fatal(err)
	}
	return command
}

func runnerTestRequest(t *testing.T) contracts.SandboxRunnerRequest {
	t.Helper()
	profile := contracts.SandboxRunnerProfile{
		Backend: string(contracts.SandboxBackendOCI), TimeoutMS: 5000,
		MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
		MaxArgCount: 16, MaxArgBytes: 1024, MaxEnvEntries: 8, MaxEnvBytes: 1024,
		CPUQuotaMilli: 500, MemoryBytes: 64 << 20, PIDsLimit: 32, ScratchBytes: 8 << 20,
		ImageDigest:     "sha256:" + strings.Repeat("a", 64),
		ImagePlatform:   contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
		ReadOnlyWorkdir: true, ReadOnlyRootFS: true,
	}
	if err := profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	definitionHash := contracts.HashStrings("definition")
	profileHash := contracts.HashStrings("profile")
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   "workspace-a", ToolRunID: "tool-run-a", ToolAttempt: 1,
		Backend: contracts.SandboxBackendOCI, OperationID: "operation-a", OperationOwnerID: "owner-a", OperationFence: 1,
		AttemptID: "attempt-a", EffectID: "effect-a",
		ToolRequestHash: contracts.HashStrings("tool request"), OperationRequestHash: contracts.HashStrings("operation request"),
		EffectReservationHash: contracts.HashStrings("reservation"), ToolDefinitionHash: definitionHash,
		SandboxProfileHash: profileHash, QualificationHash: contracts.HashStrings("qualification"),
	}
	return contracts.SandboxRunnerRequest{
		SchemaVersion: contracts.SandboxRunnerProtocolVersion,
		RequestID:     "toolreq-runner-transport", WorkspaceID: "workspace-a",
		WorkspaceMount: contracts.WorkspaceMountRef{
			ID: "wsmount_" + strings.Repeat("b", 32), WorkspaceID: "workspace-a",
		},
		Execution: identity, ToolID: "repository.status", ToolDefinitionHash: definitionHash,
		SandboxProfileHash: profileHash, RunnerProfileHash: profile.Hash(),
		Argv:             []string{"status", "--short"},
		WorkingDirectory: "src", Profile: profile,
	}
}

func TestUnixRunnerTransportAuthenticatesBoundsAndReconciles(t *testing.T) {
	requireUnixSocket(t)
	token := strings.Repeat("a", sha256.Size*2)
	runtime := &fakeRuntime{}
	socket := shortRunnerSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- Listen(ctx, socket, token, runtime) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("runner listener: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("runner listener did not stop after cancellation")
		}
	})

	client, err := NewClient(socket, token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readyCancel()
	for {
		if err := client.Health(readyCtx); err == nil {
			break
		}
		if readyCtx.Err() != nil {
			t.Fatal("authenticated runner handshake did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	requestBody, err := runnerTestRequest(t).MarshalBounded()
	if err != nil {
		t.Fatal(err)
	}
	for name, credential := range map[string]string{"missing": "", "wrong": strings.Repeat("f", sha256.Size*2)} {
		status := unixRunnerStatus(t, socket, credential, requestBody)
		if status != http.StatusUnauthorized {
			t.Errorf("%s IPC credential status = %d, want %d", name, status, http.StatusUnauthorized)
		}
	}
	if runs, _ := runtime.counts(); runs != 0 {
		t.Fatalf("unauthenticated requests reached runtime %d times", runs)
	}

	request := runnerTestRequest(t)
	response, err := client.RunAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}
	if string(response.Stdout) != "bounded result" {
		t.Fatalf("unexpected runner output: %q", response.Stdout)
	}
	observation, err := client.ReconcileAttempt(context.Background(), request.Execution)
	if err != nil || observation.State != contracts.SandboxAttemptUnknown {
		t.Fatalf("reconcile = %+v, %v", observation, err)
	}
	cleanup, err := client.CleanupAttempt(context.Background(), runnerCleanupCommand(t))
	if err != nil || cleanup.State != contracts.SandboxCleanupAlreadyAbsent {
		t.Fatalf("cleanup = %+v, %v", cleanup, err)
	}
	runs, reconciles := runtime.counts()
	if runs != 1 || reconciles != 1 || runtime.cleanupCount() != 1 {
		t.Fatalf("runtime calls run=%d reconcile=%d cleanup=%d, want one each", runs, reconciles, runtime.cleanupCount())
	}
}

func unixRunnerStatus(t *testing.T, socket, token string, body []byte) int {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
	}}
	client := &http.Client{Transport: transport}
	t.Cleanup(transport.CloseIdleConnections)
	request, err := http.NewRequest(http.MethodPost, "http://fornix.runner"+runnerRunPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("Unix socket authentication probe: %v", err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestRunnerShutdownBoundsWaitForUnresponsiveRuntime(t *testing.T) {
	token := strings.Repeat("9", sha256.Size*2)
	runtime := &uninterruptibleRuntime{
		fakeRuntime: &fakeRuntime{}, started: make(chan struct{}),
		release: make(chan struct{}), returned: make(chan struct{}),
	}
	listener := newPipeListener()
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		handler, err := NewHandler(runtime, token)
		if err != nil {
			serveDone <- err
			return
		}
		serveDone <- serveListenerWithTimeout(ctx, listener, handler, 20*time.Millisecond)
	}()

	serverConnection, clientConnection := net.Pipe()
	listener.connections <- serverConnection
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return clientConnection, nil
	}}
	client := &http.Client{Transport: transport}
	requestBody, err := runnerTestRequest(t).MarshalBounded()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "http://fornix.runner"+runnerRunPath, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	requestDone := make(chan struct{})
	go func() {
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
		}
		close(requestDone)
	}()
	select {
	case <-runtime.started:
	case <-time.After(time.Second):
		t.Fatal("runtime call did not start")
	}
	cancel()
	time.Sleep(60 * time.Millisecond)
	select {
	case err := <-serveDone:
		if !errors.Is(err, ErrRunnerUnavailable) {
			t.Fatalf("listener shutdown with an unresponsive runtime = %v, want unavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener hung instead of reporting a bounded shutdown failure")
	}
	select {
	case <-runtime.returned:
		t.Fatal("test runtime returned before its explicit release")
	default:
	}
	close(runtime.release)
	select {
	case <-runtime.returned:
	case <-time.After(time.Second):
		t.Fatal("runtime call did not return after release")
	}
	client.CloseIdleConnections()
	_ = clientConnection.Close()
	_ = serverConnection.Close()
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("IPC client did not finish after listener shutdown")
	}
	if runs, _ := runtime.fakeRuntime.counts(); runs != 1 {
		t.Fatalf("shutdown caused %d runtime effects; want one", runs)
	}
}

func TestRunnerHandlerRejectsUnauthenticatedAndMalformedRequests(t *testing.T) {
	token := strings.Repeat("b", sha256.Size*2)
	runtime := &fakeRuntime{}
	handler, err := NewHandler(runtime, token)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		path        string
		authorize   bool
		body        string
		contentType string
		wantStatus  int
	}{
		{name: "unauthorized", path: runnerRunPath, body: `{}`, contentType: "application/json", wantStatus: http.StatusUnauthorized},
		{name: "unknown fields", path: runnerRunPath, authorize: true, body: `{"untrusted":true}`, contentType: "application/json", wantStatus: http.StatusBadRequest},
		{name: "wrong content type", path: runnerRunPath, authorize: true, body: `{}`, contentType: "text/plain", wantStatus: http.StatusBadRequest},
		{name: "unknown route", path: "/v1/other", authorize: true, wantStatus: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			if test.authorize {
				request.Header.Set("Authorization", "Bearer "+token)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.wantStatus, response.Body.String())
			}
		})
	}
	runs, reconciles := runtime.counts()
	if runs != 0 || reconciles != 0 || runtime.cleanupCount() != 0 {
		t.Fatalf("invalid requests reached runtime: run=%d reconcile=%d cleanup=%d", runs, reconciles, runtime.cleanupCount())
	}
}

func TestRunnerCleanupFailsClosedOnInvalidAndUncertainObservations(t *testing.T) {
	token := strings.Repeat("7", sha256.Size*2)
	runtime := &fakeRuntime{}
	handler, err := NewHandler(runtime, token)
	if err != nil {
		t.Fatal(err)
	}
	command := runnerCleanupCommand(t)
	body, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	badRequest := append([]byte(nil), body...)
	badRequest = bytes.Replace(badRequest, []byte(command.IntentHash), []byte(contracts.HashStrings("wrong intent")), 1)
	request := httptest.NewRequest(http.MethodPost, runnerCleanupPath, bytes.NewReader(badRequest))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || runtime.cleanupCount() != 0 {
		t.Fatalf("invalid cleanup command status=%d calls=%d body=%s", response.Code, runtime.cleanupCount(), response.Body.String())
	}

	runtime.badCleanup = true
	request = httptest.NewRequest(http.MethodPost, runnerCleanupPath, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "different cleanup") {
		t.Fatalf("mismatched cleanup observation response=%d %q", response.Code, response.Body.String())
	}
	if runtime.cleanupCount() != 1 {
		t.Fatalf("valid cleanup command reached runtime %d times, want once", runtime.cleanupCount())
	}
}

func TestRunnerHandlerBoundsRequestsAndHidesRuntimeDiagnostics(t *testing.T) {
	token := strings.Repeat("d", sha256.Size*2)
	runtime := &fakeRuntime{}
	handler, err := NewHandler(runtime, token)
	if err != nil {
		t.Fatal(err)
	}
	request := runnerTestRequest(t)
	body, err := request.MarshalBounded()
	if err != nil {
		t.Fatal(err)
	}

	oversized := httptest.NewRequest(http.MethodPost, runnerRunPath, strings.NewReader(strings.Repeat("x", contracts.MaxSandboxRunnerRequestBytes+1)))
	oversized.Header.Set("Content-Type", "application/json")
	oversized.Header.Set("Authorization", "Bearer "+token)
	oversizedResponse := httptest.NewRecorder()
	handler.ServeHTTP(oversizedResponse, oversized)
	if oversizedResponse.Code != http.StatusBadRequest {
		t.Fatalf("oversized request status = %d, want %d", oversizedResponse.Code, http.StatusBadRequest)
	}
	if runs, _ := runtime.counts(); runs != 0 {
		t.Fatalf("oversized request reached runtime %d times", runs)
	}

	runtime.mu.Lock()
	runtime.runErr = errors.New("private-host-path and sentinel-secret")
	runtime.mu.Unlock()
	valid := httptest.NewRequest(http.MethodPost, runnerRunPath, bytes.NewReader(body))
	valid.Header.Set("Content-Type", "application/json")
	valid.Header.Set("Authorization", "Bearer "+token)
	validResponse := httptest.NewRecorder()
	handler.ServeHTTP(validResponse, valid)
	if validResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("runtime error status = %d, want %d", validResponse.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(validResponse.Body.String(), "private-host-path") || strings.Contains(validResponse.Body.String(), "sentinel-secret") {
		t.Fatalf("runtime diagnostic leaked in response: %q", validResponse.Body.String())
	}
	if runs, _ := runtime.counts(); runs != 1 {
		t.Fatalf("valid request reached runtime %d times, want one", runs)
	}
}

func TestRunnerHandlerEnforcesConcurrentExecutionLimit(t *testing.T) {
	token := strings.Repeat("e", sha256.Size*2)
	runtime := &blockingRuntime{
		fakeRuntime: &fakeRuntime{}, started: make(chan struct{}, maxRunnerConcurrent), release: make(chan struct{}),
	}
	handler, err := NewHandler(runtime, token)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-runtime.release:
		default:
			close(runtime.release)
		}
	}()

	requestBody, err := runnerTestRequest(t).MarshalBounded()
	if err != nil {
		t.Fatal(err)
	}
	responses := make(chan *httptest.ResponseRecorder, maxRunnerConcurrent)
	for i := 0; i < maxRunnerConcurrent; i++ {
		request := httptest.NewRequest(http.MethodPost, runnerRunPath, bytes.NewReader(requestBody))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		go func(request *http.Request) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			responses <- response
		}(request)
	}
	for i := 0; i < maxRunnerConcurrent; i++ {
		select {
		case <-runtime.started:
		case <-time.After(2 * time.Second):
			t.Fatal("not all configured runtime slots started")
		}
	}

	overflowRequest := httptest.NewRequest(http.MethodPost, runnerRunPath, bytes.NewReader(requestBody))
	overflowRequest.Header.Set("Content-Type", "application/json")
	overflowRequest.Header.Set("Authorization", "Bearer "+token)
	overflowResponse := httptest.NewRecorder()
	handler.ServeHTTP(overflowResponse, overflowRequest)
	if overflowResponse.Code != http.StatusServiceUnavailable || !strings.Contains(overflowResponse.Body.String(), "runner_busy") {
		t.Fatalf("saturated handler response = %d %q", overflowResponse.Code, overflowResponse.Body.String())
	}
	if runs, _ := runtime.counts(); runs != 0 {
		t.Fatalf("blocked calls reached the fake runtime %d times", runs)
	}

	close(runtime.release)
	for i := 0; i < maxRunnerConcurrent; i++ {
		select {
		case response := <-responses:
			if response.Code != http.StatusOK {
				t.Errorf("admitted request status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("admitted request did not finish")
		}
	}
	if runs, _ := runtime.counts(); runs != maxRunnerConcurrent {
		t.Fatalf("runtime calls = %d, want exactly %d", runs, maxRunnerConcurrent)
	}
}

func TestRunnerClientNeverFollowsProtocolRedirects(t *testing.T) {
	client, err := NewClient("/private/tmp/runner.sock", strings.Repeat("f", sha256.Size*2))
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	calls := 0
	client.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.URL.Host != "fornix.runner" || request.URL.Path != runnerRunPath {
			t.Fatalf("unexpected protocol endpoint: %s %s", request.URL.Host, request.URL.Path)
		}
		if request.Header.Get("Authorization") == "" {
			t.Fatal("IPC credential missing from request")
		}
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Header:     http.Header{"Location": []string{"https://outside.example/collect"}},
			Body:       io.NopCloser(strings.NewReader("redirect")),
			Request:    request,
		}, nil
	})
	if err := client.post(context.Background(), runnerRunPath, []byte(`{}`), 1024, new(any)); !errors.Is(err, ErrRunnerUnavailable) {
		t.Fatalf("redirect response error = %v, want unavailable", err)
	}
	if calls != 1 {
		t.Fatalf("redirect caused %d transport requests, want exactly one", calls)
	}
}

func TestRunnerTransportFailsClosedOnRuntimeErrorAndMismatchedResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*fakeRuntime)
	}{
		{name: "uncertain runtime error", edit: func(runtime *fakeRuntime) { runtime.runErr = errors.New("private host path and secret-value") }},
		{name: "mismatched request hash", edit: func(runtime *fakeRuntime) { runtime.badResponse = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireUnixSocket(t)
			token := strings.Repeat("c", sha256.Size*2)
			runtime := &fakeRuntime{}
			test.edit(runtime)
			socket := shortRunnerSocket(t)
			ctx, cancel := context.WithCancel(context.Background())
			serveDone := make(chan error, 1)
			go func() { serveDone <- Listen(ctx, socket, token, runtime) }()
			defer func() {
				cancel()
				select {
				case err := <-serveDone:
					if err != nil {
						t.Errorf("runner listener: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Error("runner listener did not stop")
				}
			}()
			client, err := NewClient(socket, token)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			readyCtx, readyCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer readyCancel()
			for client.Health(readyCtx) != nil && readyCtx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			response, err := client.RunAttempt(context.Background(), runnerTestRequest(t))
			if !errors.Is(err, ErrRunnerUnavailable) || response.RequestID != "" {
				t.Fatalf("run should fail closed: response=%+v err=%v", response, err)
			}
			if strings.Contains(fmt.Sprint(err), "secret-value") || strings.Contains(fmt.Sprint(err), "private host path") {
				t.Fatal("runtime diagnostic leaked through the client error")
			}
			runs, _ := runtime.counts()
			if runs != 1 {
				t.Fatalf("runtime was invoked %d times; transport must not retry ambiguous execution", runs)
			}
		})
	}
}

func TestRunnerSocketRefusesNonSocketOccupant(t *testing.T) {
	socket := shortRunnerSocket(t)
	if err := os.WriteFile(socket, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenUnix(socket); err == nil {
		t.Fatal("listener replaced a non-socket file")
	}
	body, err := os.ReadFile(socket)
	if err != nil || string(body) != "preserve" {
		t.Fatalf("non-socket path was modified: body=%q err=%v", body, err)
	}
}

func TestRunnerSocketDoesNotChangeUnsafeParentPermissions(t *testing.T) {
	parent := shortRunnerSocket(t)
	if err := os.Remove(parent); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := listenUnix(filepath.Join(parent, "runner.sock")); err == nil {
		t.Fatal("listener accepted a group/world-accessible parent directory")
	}
	info, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("unsafe parent permissions changed to %#o; want %#o", got, 0o750)
	}
}

func TestRunnerSocketRejectsReplaceableAncestor(t *testing.T) {
	base := filepath.Dir(shortRunnerSocket(t))
	unsafe := filepath.Join(base, "replaceable")
	leaf := filepath.Join(unsafe, "private")
	if err := os.MkdirAll(leaf, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafe, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(leaf, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateSocketDirectory(leaf); err == nil {
		t.Fatal("socket directory accepted an ancestor replaceable by other users")
	}
	if got, err := os.Stat(unsafe); err != nil || got.Mode().Perm() != 0o777 {
		t.Fatalf("unsafe ancestor was changed: info=%v err=%v", got, err)
	}
}

func TestRunnerSocketDoesNotCreateThroughSymlinkParent(t *testing.T) {
	base := filepath.Dir(shortRunnerSocket(t))
	target := filepath.Join(base, "socket-target")
	alias := filepath.Join(base, "socket-alias")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	created := filepath.Join(target, "must-not-create")
	socket := filepath.Join(alias, "must-not-create", "runner.sock")
	if _, err := listenUnix(socket); err == nil {
		t.Fatal("listener accepted a symlink parent")
	}
	if _, err := os.Lstat(created); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("listener mutated symlink target before rejecting it: lstat err=%v", err)
	}
}
