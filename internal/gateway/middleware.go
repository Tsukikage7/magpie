package gateway

import (
	"net/http"

	"github.com/yetone/magpie/internal/middleware"
	"github.com/yetone/magpie/internal/plugin"
	"github.com/yetone/magpie/internal/provider"
)

// serveAgent is serve for a request an agent sent: the gateway middleware
// plugins (internal/middleware) see its body first, and its reply on the
// way back, in the agent's own API. magpie's own requests through serve
// (titles, the router's classifier, search and vision stand-ins) go
// without them, as their replies are magpie's to read.
func (s *Server) serveAgent(w http.ResponseWriter, r *http.Request, from provider.Protocol, body []byte) {
	s.serveAgentWith(w, r, from, body, s.serve)
}

type gatewayServe func(http.ResponseWriter, *http.Request, provider.Protocol, []byte)

// Native sign-in relays keep their authentication and retry path while using
// the same agent lifecycle as requests routed to a provider managed by magpie.
func (s *Server) serveAgentWith(w http.ResponseWriter, r *http.Request, from provider.Protocol, body []byte, serve gatewayServe) {
	withGatewaySession(w, r)
	w, recorded := recordConversation(w, r, from, body)
	defer recorded()
	var hooks *plugin.GatewayRun
	var info plugin.GatewayContext
	if plugin.HasGateway() {
		info = plugin.GatewayContext{Protocol: string(from), Model: modelOf(body), Stream: streamOf(body), Path: r.URL.Path, Agent: agentOf(r), Session: sessionOf(r.Header)}
		if req, err := parse(from, body); err == nil {
			info.Cwd = workDir(req)
			info.Query = gatewayQuery(req)
		}
		hooks = plugin.GatewayBegin(r.Context(), body, info)
		if hooks != nil {
			defer hooks.Close()
			body = hooks.Body
		}
	}
	if hooks != nil {
		var done func()
		w, done = gatewayResponse(w, r, from, body, hooks)
		defer done()
	}
	run := middleware.Begin(middleware.Info{Protocol: string(from), Model: modelOf(body), Stream: streamOf(body), Path: r.URL.Path, Agent: agentOf(r)})
	if run == nil {
		serve(w, r, from, body)
		return
	}
	defer run.End()
	body, turned := run.Request(body)
	if turned != nil {
		writeError(w, from, turned.Status, turned.Message)
		return
	}
	w, done := run.Wrap(w)
	defer done()
	serve(w, r, from, body)
}

// gatewayQuery uses the same user-authored words as routing, excluding
// agent context, tool results and images. Tool-only increments have none.
func gatewayQuery(req *Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		m := req.Messages[i]
		if m.Role != "user" {
			continue
		}
		if query := userSaid(m); query != "" {
			return query
		}
	}
	return ""
}
