package gateway

import (
	"encoding/json"
	"net/http"

	"github.com/yetone/magpie/internal/plugin"
	"github.com/yetone/magpie/internal/provider"
)

// gatewayResponse observes the bytes delivered to the client without changing
// them. The caller runs done after middleware has finished writing its reply.
func gatewayResponse(w http.ResponseWriter, r *http.Request, proto provider.Protocol, body []byte, run *plugin.GatewayRun) (http.ResponseWriter, func()) {
	capture := &conversationWriter{captureResponseWriter: &captureResponseWriter{
		ResponseWriter: w, full: &spool{limit: conversationBodyMax, pattern: "magpie-hook-*"},
	}}
	return capture, func() {
		defer capture.full.discard()
		if streamOf(body) {
			_ = http.NewResponseController(w).Flush()
		}
		output, err := capture.full.read()
		if err != nil || capture.full.cut() || capture.status >= 400 || r.Context().Err() != nil {
			run.Finish(r.Context(), nil, false, body)
			return
		}
		reply, err := gatewayReply(proto, output)
		var response obj
		json.Unmarshal(reply, &response)
		success := err == nil && (len(response["error"]) == 0 || string(response["error"]) == "null") && str(response, "status") != "failed" && str(response, "status") != "incomplete"
		run.Finish(r.Context(), reply, success, body)
	}
}
