package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/yetone/magpie/internal/provider"
)

// gatewayReply collapses a complete client-facing stream without translating
// its parts through the IR. This copy is only for completion observers;
// the client continues to receive the original stream as it arrives.
func gatewayReply(proto provider.Protocol, body []byte) (json.RawMessage, error) {
	body = bytes.TrimSpace(body)
	if json.Valid(body) {
		var object obj
		if err := json.Unmarshal(body, &object); err != nil || object == nil {
			return nil, errors.New("invalid model reply")
		}
		return bytes.Clone(body), nil
	}
	a := newGatewayStream(proto)
	err := readSSE(bytes.NewReader(body), a.add)
	if err != nil {
		return nil, err
	}
	if !a.ended {
		return nil, errors.New("incomplete model stream")
	}
	return a.finish()
}

type gatewayStream struct {
	proto     provider.Protocol
	root      obj
	items     map[int]obj
	calls     map[int]map[int]obj
	fragments map[int]string
	ended     bool
}

func newGatewayStream(proto provider.Protocol) gatewayStream {
	return gatewayStream{proto: proto, root: obj{}, items: map[int]obj{}, calls: map[int]map[int]obj{}, fragments: map[int]string{}}
}

func gatewayIndex(m obj, key string) (int, error) {
	var n int
	if b := m[key]; len(b) > 0 && json.Unmarshal(b, &n) != nil {
		return 0, errors.New("invalid stream index")
	}
	if n < 0 || n > 65535 {
		return 0, errors.New("invalid stream index")
	}
	return n, nil
}

func gatewaySorted(items map[int]obj) []obj {
	out := make([]obj, 0, len(items))
	for _, key := range slices.Sorted(maps.Keys(items)) {
		out = append(out, items[key])
	}
	return out
}

func gatewayAppendText(dst obj, key string, text string) { dst[key] = raw(str(dst, key) + text) }

func (a *gatewayStream) add(event, data string) error {
	if data == "[DONE]" {
		if a.proto == provider.Chat {
			a.ended = true
		}
		return nil
	}
	var v obj
	if json.Unmarshal([]byte(data), &v) != nil || v == nil {
		return errors.New("invalid model event")
	}
	if err := v["error"]; len(err) > 0 && string(err) != "null" {
		return errors.New("failed model stream")
	}
	switch a.proto {
	case provider.Chat:
		return a.chat(v)
	case provider.Responses:
		return a.responses(v, event)
	case provider.Gemini:
		return a.gemini(v)
	default:
		return a.anthropic(v, event)
	}
}

func (a *gatewayStream) chat(v obj) error {
	for key, value := range v {
		if key != "choices" && key != "object" {
			a.root[key] = value
		}
	}
	var choices []obj
	if err := json.Unmarshal(v["choices"], &choices); err != nil {
		return err
	}
	for _, choice := range choices {
		at, err := gatewayIndex(choice, "index")
		if err != nil {
			return err
		}
		if a.items[at] == nil {
			a.items[at] = obj{"index": raw(at), "message": raw(obj{"role": raw("assistant"), "content": raw(nil)}), "finish_reason": raw(nil)}
		}
		c := a.items[at]
		var message, delta obj
		json.Unmarshal(c["message"], &message)
		json.Unmarshal(choice["delta"], &delta)
		if full := choice["message"]; len(full) > 0 {
			json.Unmarshal(full, &message)
		}
		for key, value := range delta {
			switch key {
			case "tool_calls":
				var calls []obj
				if json.Unmarshal(value, &calls) != nil {
					return errors.New("invalid streamed calls")
				}
				if a.calls[at] == nil {
					a.calls[at] = map[int]obj{}
				}
				for _, call := range calls {
					i, err := gatewayIndex(call, "index")
					if err != nil {
						return err
					}
					if a.calls[at][i] == nil {
						a.calls[at][i] = obj{"type": raw("function"), "function": raw(obj{})}
					}
					stored := a.calls[at][i]
					for k, b := range call {
						if k != "index" && k != "function" {
							stored[k] = b
						}
					}
					var function, fragment obj
					json.Unmarshal(stored["function"], &function)
					json.Unmarshal(call["function"], &fragment)
					for k, b := range fragment {
						if k == "name" || k == "arguments" {
							gatewayAppendText(function, k, str(fragment, k))
						} else {
							function[k] = b
						}
					}
					stored["function"] = raw(function)
				}
			case "function_call":
				var function, fragment obj
				json.Unmarshal(message[key], &function)
				json.Unmarshal(value, &fragment)
				if function == nil {
					function = obj{}
				}
				for k := range fragment {
					gatewayAppendText(function, k, str(fragment, k))
				}
				message[key] = raw(function)
			case "content", "reasoning", "reasoning_content", "refusal":
				var text string
				if json.Unmarshal(value, &text) == nil {
					gatewayAppendText(message, key, text)
				} else if string(value) != "null" {
					message[key] = value
				}
			default:
				message[key] = value
			}
		}
		if len(a.calls[at]) > 0 {
			message["tool_calls"] = raw(gatewaySorted(a.calls[at]))
		}
		c["message"] = raw(message)
		if reason := choice["finish_reason"]; len(reason) > 0 && string(reason) != "null" {
			c["finish_reason"] = reason
		}
		if b := choice["logprobs"]; len(b) > 0 {
			c["logprobs"] = b
		}
	}
	return nil
}

func (a *gatewayStream) responses(v obj, event string) error {
	typ := str(v, "type")
	if typ == "" {
		typ = event
	}
	switch typ {
	case "response.failed", "response.incomplete", "error":
		return errors.New("incomplete Responses stream")
	case "response.created", "response.in_progress":
		var r obj
		if json.Unmarshal(v["response"], &r) == nil {
			maps.Copy(a.root, r)
		}
	case "response.output_item.added", "response.output_item.done":
		i, err := gatewayIndex(v, "output_index")
		if err != nil {
			return err
		}
		var item obj
		if json.Unmarshal(v["item"], &item) != nil || item == nil {
			return errors.New("invalid Responses item")
		}
		a.items[i] = item
	case "response.completed":
		var r obj
		if json.Unmarshal(v["response"], &r) != nil || r == nil {
			return errors.New("invalid completed response")
		}
		maps.Copy(a.root, r)
		a.ended = true
		var items []obj
		json.Unmarshal(r["output"], &items)
		if len(items) > 0 {
			a.items = map[int]obj{}
			for i, item := range items {
				a.items[i] = item
			}
		}
	case "response.content_part.added", "response.content_part.done", "response.output_text.delta", "response.output_text.done", "response.refusal.delta", "response.refusal.done", "response.function_call_arguments.delta", "response.function_call_arguments.done":
		i, err := gatewayIndex(v, "output_index")
		if err != nil {
			return err
		}
		item := a.items[i]
		if item == nil {
			item = obj{"type": raw("message"), "role": raw("assistant")}
			if typ == "response.function_call_arguments.delta" || typ == "response.function_call_arguments.done" {
				item["type"] = raw("function_call")
			}
			a.items[i] = item
		}
		if str(v, "item_id") != "" {
			item["id"] = v["item_id"]
		}
		if strings.HasPrefix(typ, "response.function_call_arguments.") {
			if strings.HasSuffix(typ, "delta") {
				gatewayAppendText(item, "arguments", str(v, "delta"))
			} else {
				item["arguments"] = v["arguments"]
			}
			return nil
		}
		j, err := gatewayIndex(v, "content_index")
		if err != nil {
			return err
		}
		var parts []obj
		json.Unmarshal(item["content"], &parts)
		for len(parts) <= j {
			parts = append(parts, obj{})
		}
		if strings.HasPrefix(typ, "response.content_part.") {
			json.Unmarshal(v["part"], &parts[j])
		} else {
			field, kind := "text", "output_text"
			if strings.HasPrefix(typ, "response.refusal.") {
				field, kind = "refusal", "refusal"
			}
			parts[j]["type"] = raw(kind)
			if strings.HasSuffix(typ, "delta") {
				gatewayAppendText(parts[j], field, str(v, "delta"))
			} else {
				parts[j][field] = v[field]
			}
		}
		item["content"] = raw(parts)
	}
	return nil
}

func (a *gatewayStream) anthropic(v obj, event string) error {
	typ := str(v, "type")
	if typ == "" {
		typ = event
	}
	switch typ {
	case "error":
		return errors.New("failed Anthropic stream")
	case "message_start":
		var message obj
		if json.Unmarshal(v["message"], &message) != nil || message == nil {
			return errors.New("invalid Anthropic start")
		}
		maps.Copy(a.root, message)
	case "content_block_start":
		i, err := gatewayIndex(v, "index")
		if err != nil {
			return err
		}
		var block obj
		if json.Unmarshal(v["content_block"], &block) != nil || block == nil {
			return errors.New("invalid Anthropic block")
		}
		a.items[i] = block
	case "content_block_delta":
		i, err := gatewayIndex(v, "index")
		if err != nil {
			return err
		}
		block := a.items[i]
		if block == nil {
			return errors.New("missing Anthropic block")
		}
		var delta obj
		json.Unmarshal(v["delta"], &delta)
		switch str(delta, "type") {
		case "text_delta":
			gatewayAppendText(block, "text", str(delta, "text"))
		case "thinking_delta":
			gatewayAppendText(block, "thinking", str(delta, "thinking"))
		case "signature_delta":
			gatewayAppendText(block, "signature", str(delta, "signature"))
		case "input_json_delta":
			a.fragments[i] += str(delta, "partial_json")
		case "citations_delta":
			var cites []json.RawMessage
			json.Unmarshal(block["citations"], &cites)
			cites = append(cites, delta["citation"])
			block["citations"] = raw(cites)
		}
	case "message_delta":
		var delta obj
		json.Unmarshal(v["delta"], &delta)
		maps.Copy(a.root, delta)
		var usage, current obj
		json.Unmarshal(a.root["usage"], &usage)
		json.Unmarshal(v["usage"], &current)
		if usage == nil {
			usage = obj{}
		}
		maps.Copy(usage, current)
		a.root["usage"] = raw(usage)
	case "message_stop":
		a.ended = true
	}
	return nil
}

func (a *gatewayStream) gemini(v obj) error {
	for key, value := range v {
		if key != "candidates" {
			a.root[key] = value
		}
	}
	var candidates []obj
	if json.Unmarshal(v["candidates"], &candidates) != nil {
		return errors.New("invalid Gemini candidates")
	}
	for _, candidate := range candidates {
		i, err := gatewayIndex(candidate, "index")
		if err != nil {
			return err
		}
		if a.items[i] == nil {
			a.items[i] = obj{"index": raw(i)}
		}
		stored := a.items[i]
		for key, value := range candidate {
			if key != "content" {
				stored[key] = value
			}
		}
		var content, fragment obj
		json.Unmarshal(stored["content"], &content)
		json.Unmarshal(candidate["content"], &fragment)
		if content == nil {
			content = obj{"role": raw("model")}
		}
		var before, parts []obj
		json.Unmarshal(content["parts"], &before)
		json.Unmarshal(fragment["parts"], &parts)
		for _, part := range parts {
			if len(before) > 0 && len(part["text"]) > 0 && len(part["thoughtSignature"]) == 0 {
				last := before[len(before)-1]
				if len(last["text"]) > 0 && bytes.Equal(last["thought"], part["thought"]) && len(last["thoughtSignature"]) == 0 {
					gatewayAppendText(last, "text", str(part, "text"))
					continue
				}
			}
			before = append(before, part)
		}
		if fragment["role"] != nil {
			content["role"] = fragment["role"]
		}
		content["parts"] = raw(before)
		stored["content"] = raw(content)
		if str(candidate, "finishReason") != "" {
			a.ended = true
		}
	}
	return nil
}

func (a *gatewayStream) finish() (json.RawMessage, error) {
	switch a.proto {
	case provider.Chat:
		if len(a.items) == 0 {
			return nil, errors.New("empty Chat stream")
		}
		for _, choice := range a.items {
			if str(choice, "finish_reason") == "" {
				return nil, errors.New("unfinished Chat choice")
			}
		}
		a.root["object"], a.root["choices"] = raw("chat.completion"), raw(gatewaySorted(a.items))
	case provider.Responses:
		a.root["object"], a.root["status"], a.root["output"] = raw("response"), raw("completed"), raw(gatewaySorted(a.items))
	case provider.Gemini:
		a.root["candidates"] = raw(gatewaySorted(a.items))
	default:
		for index, fragment := range a.fragments {
			if !json.Valid([]byte(fragment)) {
				return nil, errors.New("unfinished Anthropic tool arguments")
			}
			a.items[index]["input"] = json.RawMessage(fragment)
		}
		a.root["type"], a.root["role"], a.root["content"] = raw("message"), raw("assistant"), raw(gatewaySorted(a.items))
	}
	return raw(a.root), nil
}
