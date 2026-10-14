package plugin

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Gateway is the asynchronous gateway entry a package runs in Bun. Unlike
// middleware, it can await I/O and observe the completed exchange.
func Gateway(target string) string {
	st, err := os.Stat(target)
	if err != nil {
		return ""
	}
	if !st.IsDir() {
		base := strings.ToLower(filepath.Base(target))
		// Bun runs TypeScript directly, so a hook may be a .ts file.
		for _, ext := range []string{".gateway.js", ".gateway.mjs", ".gateway.ts", ".gateway.mts"} {
			if strings.HasSuffix(base, ext) {
				return target
			}
		}
		return ""
	}
	var pkg struct {
		Magpie struct {
			Gateway string `json:"gateway"`
		} `json:"magpie"`
	}
	b, err := os.ReadFile(filepath.Join(target, "package.json"))
	if err != nil || json.Unmarshal(b, &pkg) != nil || strings.TrimSpace(pkg.Magpie.Gateway) == "" {
		return ""
	}
	return filepath.Join(target, filepath.FromSlash(strings.TrimSpace(pkg.Magpie.Gateway)))
}

// GatewayOnly distinguishes request hooks from packages that also serve a
// provider or add an agent. Keep the provider entry rules aligned with Bun.
func GatewayOnly(target string) bool {
	if Gateway(target) == "" {
		return false
	}
	st, err := os.Stat(target)
	if err != nil {
		return false
	}
	if !st.IsDir() {
		return true
	}
	var pkg struct {
		Main    string
		Exports json.RawMessage
	}
	b, err := os.ReadFile(filepath.Join(target, "package.json"))
	if err != nil || json.Unmarshal(b, &pkg) != nil || pkg.Main != "" || len(pkg.Exports) != 0 {
		return false
	}
	if file, _ := Agent(target); file != "" {
		return false
	}
	for _, name := range []string{"index.ts", "index.tsx", "index.js", "index.mjs", "index.cjs"} {
		if _, err := os.Stat(filepath.Join(target, name)); err == nil {
			return false
		}
	}
	return true
}

// GatewayContext contains only the request identity and text, never its
// authorization headers. Empty fields mean the agent supplied no identity.
type GatewayContext struct {
	Protocol string `json:"protocol"`
	Model    string `json:"model"`
	Stream   bool   `json:"stream"`
	Path     string `json:"path"`
	Agent    string `json:"agent"`
	Session  string `json:"session"`
	Cwd      string `json:"cwd"`
	Query    string `json:"query"`
}

// GatewayState is aggregate diagnostics for this host's lifetime. Outcomes
// are fixed codes; no request text, credentials or private context is kept.
type GatewayState struct {
	Calls       uint64  `json:"calls"`
	Failures    uint64  `json:"failures"`
	AvgMillis   float64 `json:"avgMillis"`
	LastOutcome string  `json:"lastOutcome"`
	LastBytes   uint64  `json:"lastBytes"`
}

// GatewayStates asks only an existing host, so opening Settings does not
// start a gateway hook merely to display its diagnostics.
func GatewayStates(ctx context.Context) map[string]GatewayState {
	if !HasGateway() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if !hostMu.TryLock() {
		return nil
	}
	h := current
	hostMu.Unlock()
	if h == nil {
		return nil
	}
	var states map[string]GatewayState
	if h.call(ctx, "gatewayStates", nil, &states) != nil {
		return nil
	}
	return states
}

// HasGateway avoids starting Bun for users who have no asynchronous hooks.
func HasGateway() bool {
	for _, e := range Load().Plugins {
		if !e.Off && Gateway(Target(e.Spec)) != "" {
			return true
		}
	}
	return false
}

// GatewayRun belongs to the host that began it, even if the plugin list changes
// before the response. Close releases its private state and cancels active I/O.
type GatewayRun struct {
	Body json.RawMessage

	h        *host
	id       string
	token    string
	mu       sync.Mutex
	closed   bool
	finished bool
	stop     func() bool
}

// GatewayBegin bounds startup and the entire request-hook chain. Failure or an
// absent hook returns nil so the gateway can keep the original request.
func GatewayBegin(ctx context.Context, body []byte, info GatewayContext) *GatewayRun {
	if !HasGateway() {
		return nil
	}
	start, cancel := context.WithTimeout(ctx, 11*time.Second)
	defer cancel()
	checkList()
	// Provider discovery may already be starting the shared host. Wait within
	// the same hook budget rather than skipping recall on the first request.
	wait := time.NewTicker(10 * time.Millisecond)
	defer wait.Stop()
	for !hostMu.TryLock() {
		select {
		case <-start.Done():
			return nil
		case <-wait.C:
		}
	}
	if start.Err() != nil {
		hostMu.Unlock()
		return nil
	}
	h, err := getLocked(start)
	hostMu.Unlock()
	if err != nil || start.Err() != nil {
		return nil
	}
	var out struct {
		ID    string          `json:"id"`
		Token string          `json:"token"`
		Body  json.RawMessage `json:"body"`
	}
	err = h.call(start, "gatewayRequest", map[string]any{"operation": "begin", "body": json.RawMessage(body), "context": info}, &out)
	if err != nil || out.ID == "" || out.Token == "" || len(out.Body) == 0 || out.Body[0] != '{' || !json.Valid(out.Body) {
		return nil
	}
	run := &GatewayRun{Body: out.Body, h: h, id: out.ID, token: out.Token}
	run.mu.Lock()
	run.stop = context.AfterFunc(ctx, run.Close)
	run.mu.Unlock()
	return run
}

// Finish schedules a bounded completion notification without waiting for it.
// Client cancellation after delivery does not cancel the notification. The
// host retains bounded request state; shutdown may discard pending callbacks.
func (r *GatewayRun) Finish(ctx context.Context, reply json.RawMessage, success bool, request ...json.RawMessage) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed || r.finished {
		r.mu.Unlock()
		return
	}
	r.finished, r.closed = true, true
	stop := r.stop
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
	if !json.Valid(reply) {
		reply = json.RawMessage("null")
		success = false
	}
	params := map[string]any{"operation": "finish", "id": r.id, "token": r.token, "reply": append(json.RawMessage(nil), reply...), "success": success}
	if len(request) != 0 && json.Valid(request[0]) {
		params["request"] = append(json.RawMessage(nil), request[0]...)
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 11*time.Second)
		defer cancel()
		_ = r.h.call(ctx, "gatewayRequest", params, nil)
	}()
}

// Close releases an abandoned request and cancels active plugin I/O.
func (r *GatewayRun) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	stop := r.stop
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = r.h.call(ctx, "gatewayRequest", map[string]any{"operation": "close", "id": r.id, "token": r.token}, nil)
}

// GatewayRequest preserves the request-only API for existing callers.
func GatewayRequest(ctx context.Context, body []byte, info GatewayContext) []byte {
	run := GatewayBegin(ctx, body, info)
	if run == nil {
		return body
	}
	defer run.Close()
	return run.Body
}
