package webui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
)

const (
	// MaxBridgeMessageBytes is the host-side framing cap from the A7 design.
	MaxBridgeMessageBytes = 256 << 10
	maxFrameAssetBytes    = 512 << 10
	maxFrameBundleBytes   = 2 << 20
)

// BridgeGrant is the narrow capability carried by one sandboxed frame. A
// project is mandatory so a frame cannot choose a different project in its
// request metadata.
type BridgeGrant struct {
	Methods []string `json:"methods"`
	Project string   `json:"project"`
}

// BridgeCall is a validated JSON-RPC request from a frame. The bridge does
// not dispatch it; the future server composition supplies that seam. Keeping
// validation here makes the host-side boundary testable without importing the
// active server or plugin-host packages.
type BridgeCall struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
}

// BridgeError is a stable, credential-free refusal from the frame boundary.
type BridgeError struct {
	Code    string
	Message string
}

func (e *BridgeError) Error() string { return e.Code + ": " + e.Message }

// Bridge validates one frame's messages against its immutable grant.
type Bridge struct {
	grant   BridgeGrant
	methods map[string]struct{}
}

func NewBridge(grant BridgeGrant) (*Bridge, error) {
	grant.Project = strings.TrimSpace(grant.Project)
	if grant.Project == "" || len(grant.Project) > 200 || strings.ContainsAny(grant.Project, "\r\n") {
		return nil, &BridgeError{Code: "invalid_grant", Message: "bridge project is required"}
	}
	methods := make(map[string]struct{}, len(grant.Methods))
	for _, method := range grant.Methods {
		if !validBridgeMethod(method) {
			return nil, &BridgeError{Code: "invalid_grant", Message: fmt.Sprintf("invalid bridge method %q", method)}
		}
		if bridgeMethodForbidden(method) {
			return nil, &BridgeError{Code: "invalid_grant", Message: fmt.Sprintf("bridge method %q is forbidden", method)}
		}
		if _, exists := methods[method]; exists {
			return nil, &BridgeError{Code: "invalid_grant", Message: fmt.Sprintf("duplicate bridge method %q", method)}
		}
		methods[method] = struct{}{}
	}
	grant.Methods = append([]string(nil), grant.Methods...)
	sort.Strings(grant.Methods)
	return &Bridge{grant: grant, methods: methods}, nil
}

func (b *Bridge) Grant() BridgeGrant {
	if b == nil {
		return BridgeGrant{}
	}
	grant := b.grant
	grant.Methods = append([]string(nil), grant.Methods...)
	return grant
}

func (b *Bridge) Allows(method string) bool {
	if b == nil {
		return false
	}
	_, ok := b.methods[method]
	return ok
}

// Authorize parses and checks one JSON-RPC message. It requires the host to
// inject `_meta.project`; a frame cannot supply credentials, URLs, or a second
// project and have them forwarded to the server.
func (b *Bridge) Authorize(data []byte) (BridgeCall, error) {
	if b == nil {
		return BridgeCall{}, &BridgeError{Code: "bridge_unavailable", Message: "bridge is unavailable"}
	}
	if len(data) == 0 || len(data) > MaxBridgeMessageBytes {
		return BridgeCall{}, &BridgeError{Code: "message_too_large", Message: "bridge message exceeds the framing limit"}
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return BridgeCall{}, &BridgeError{Code: "invalid_message", Message: "bridge message is not a JSON-RPC request"}
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return BridgeCall{}, &BridgeError{Code: "invalid_message", Message: "bridge message has trailing data"}
	}
	if envelope.JSONRPC != "2.0" || !validRequestID(envelope.ID) || !validBridgeMethod(envelope.Method) {
		return BridgeCall{}, &BridgeError{Code: "invalid_message", Message: "bridge request envelope is invalid"}
	}
	if !b.Allows(envelope.Method) || bridgeMethodForbidden(envelope.Method) {
		return BridgeCall{}, &BridgeError{Code: "method_not_granted", Message: "bridge method is not granted"}
	}
	params := envelope.Params
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(params, &values); err != nil || values == nil {
		return BridgeCall{}, &BridgeError{Code: "invalid_params", Message: "bridge params must be an object"}
	}
	if err := rejectBridgeFields(values, "params"); err != nil {
		return BridgeCall{}, err
	}
	if err := checkBridgeProjects(values, b.grant.Project, "params"); err != nil {
		return BridgeCall{}, err
	}
	meta, ok := values["_meta"]
	if !ok {
		return BridgeCall{}, &BridgeError{Code: "scope_required", Message: "bridge request must carry host project metadata"}
	}
	var metaValues map[string]json.RawMessage
	if err := json.Unmarshal(meta, &metaValues); err != nil || metaValues == nil {
		return BridgeCall{}, &BridgeError{Code: "scope_required", Message: "bridge metadata is invalid"}
	}
	project, ok := metaValues["project"]
	var projectName string
	if !ok || json.Unmarshal(project, &projectName) != nil || projectName != b.grant.Project {
		return BridgeCall{}, &BridgeError{Code: "scope_denied", Message: "bridge project is outside the grant"}
	}
	if topLevel, ok := values["project"]; ok {
		var requested string
		if json.Unmarshal(topLevel, &requested) != nil || requested != b.grant.Project {
			return BridgeCall{}, &BridgeError{Code: "scope_denied", Message: "bridge project is outside the grant"}
		}
	}
	return BridgeCall{ID: append(json.RawMessage(nil), envelope.ID...), Method: envelope.Method, Params: append(json.RawMessage(nil), params...)}, nil
}

func validRequestID(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	if raw[0] == '"' {
		var value string
		return json.Unmarshal(raw, &value) == nil && value != "" && len(value) <= 128
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return false
	}
	_, ok := value.(json.Number)
	return ok
}

var rpcMethod = regexpMustCompile(`^[a-z][a-z0-9_-]*(?:\.[a-z][A-Za-z0-9_-]*)+$`)

// plug_call is the one underscore-delimited method name reserved by the
// agent-facing contract. It is still deny-by-default: a frame can use it only
// when the manifest grants that exact name.
func validBridgeMethod(method string) bool {
	return method == "plug_call" || rpcMethod.MatchString(method)
}

func bridgeMethodForbidden(method string) bool {
	if method == "plugin.plan" || method == "plugin.apply" || method == "ontology.apply" {
		return true
	}
	for _, prefix := range []string{"auth.", "network.", "http.", "secret."} {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}

var bridgeFieldNames = map[string]struct{}{
	"authorization": {}, "cookie": {}, "credentials": {}, "token": {},
	"access_token": {}, "secret": {}, "url": {}, "endpoint": {}, "origin": {},
}

func rejectBridgeFields(values map[string]json.RawMessage, location string) error {
	for key, raw := range values {
		if _, forbidden := bridgeFieldNames[strings.ToLower(key)]; forbidden {
			return &BridgeError{Code: "bridge_field_denied", Message: fmt.Sprintf("%s.%s is unavailable to UI extensions", location, key)}
		}
		if key == "project" {
			var project string
			if err := json.Unmarshal(raw, &project); err != nil || project == "" {
				return &BridgeError{Code: "scope_denied", Message: "bridge project must be a string"}
			}
		}
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(raw, &nested); err == nil && nested != nil {
			if err := rejectBridgeFields(nested, location+"."+key); err != nil {
				return err
			}
		}
		var list []json.RawMessage
		if err := json.Unmarshal(raw, &list); err == nil {
			for index, item := range list {
				var object map[string]json.RawMessage
				if err := json.Unmarshal(item, &object); err == nil && object != nil {
					if err := rejectBridgeFields(object, fmt.Sprintf("%s.%s[%d]", location, key, index)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func checkBridgeProjects(values map[string]json.RawMessage, expected, location string) error {
	for key, raw := range values {
		if key == "project" {
			var project string
			if err := json.Unmarshal(raw, &project); err != nil || project != expected {
				return &BridgeError{Code: "scope_denied", Message: fmt.Sprintf("%s.%s is outside the bridge project", location, key)}
			}
		}
		if key == "projects" {
			var projects []string
			if err := json.Unmarshal(raw, &projects); err != nil {
				return &BridgeError{Code: "scope_denied", Message: fmt.Sprintf("%s.%s must be a project list", location, key)}
			}
			for _, project := range projects {
				if project != expected {
					return &BridgeError{Code: "scope_denied", Message: fmt.Sprintf("%s.%s is outside the bridge project", location, key)}
				}
			}
		}
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(raw, &nested); err == nil && nested != nil {
			if err := checkBridgeProjects(nested, expected, location+"."+key); err != nil {
				return err
			}
		}
		var list []json.RawMessage
		if err := json.Unmarshal(raw, &list); err == nil {
			for index, item := range list {
				var object map[string]json.RawMessage
				if err := json.Unmarshal(item, &object); err == nil && object != nil {
					if err := checkBridgeProjects(object, expected, fmt.Sprintf("%s.%s[%d]", location, key, index)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// FrameBundle is a prebuilt, embedded plugin frame. Its assets are copied and
// checked at composition time; no JavaScript build, network fetch, or session
// access happens in this package.
type FrameBundle struct {
	ID     string
	Title  string
	Assets map[string][]byte
	Grant  BridgeGrant
}

type frameBundle struct {
	FrameBundle
	bridge *Bridge
}

func prepareFrameBundle(bundle FrameBundle, project string) (frameBundle, error) {
	if err := validateCatalogID(bundle.ID, "frame", 0); err != nil {
		return frameBundle{}, err
	}
	if bundle.Title == "" {
		bundle.Title = bundle.ID
	}
	if err := validateCatalogText(bundle.Title, "frame title", 0); err != nil {
		return frameBundle{}, err
	}
	if len(bundle.Assets) == 0 {
		return frameBundle{}, fmt.Errorf("webui: frame %q has no assets", bundle.ID)
	}
	if _, ok := bundle.Assets["index.html"]; !ok {
		return frameBundle{}, fmt.Errorf("webui: frame %q has no index.html", bundle.ID)
	}
	if bundle.Grant.Project == "" {
		bundle.Grant.Project = project
	}
	bridge, err := NewBridge(bundle.Grant)
	if err != nil {
		return frameBundle{}, fmt.Errorf("webui: frame %q grant: %w", bundle.ID, err)
	}
	copyAssets := make(map[string][]byte, len(bundle.Assets))
	total := 0
	for name, data := range bundle.Assets {
		if name == "" || name[0] == '/' || strings.ContainsAny(name, `\\%`) || path.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") {
			return frameBundle{}, fmt.Errorf("webui: frame %q has invalid asset path %q", bundle.ID, name)
		}
		if len(data) > maxFrameAssetBytes || total > maxFrameBundleBytes-len(data) {
			return frameBundle{}, fmt.Errorf("webui: frame %q exceeds its asset size limit", bundle.ID)
		}
		if containsFrameCapability(data) {
			return frameBundle{}, fmt.Errorf("webui: frame %q asset %q requests credentials or network egress", bundle.ID, name)
		}
		total += len(data)
		copyAssets[name] = append([]byte(nil), data...)
	}
	bundle.Assets = copyAssets
	return frameBundle{FrameBundle: bundle, bridge: bridge}, nil
}

func containsFrameCapability(data []byte) bool {
	lower := strings.ToLower(string(data))
	for _, token := range []string{
		"document.cookie", "localstorage", "sessionstorage", "indexeddb", "navigator.credentials",
		"fetch(", "xmlhttprequest", "websocket(", "eventsource(", "sendbeacon(", "http://", "https://",
	} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

const frameContentSecurityPolicy = "sandbox allow-scripts; default-src 'none'; base-uri 'none'; object-src 'none'; frame-src 'none'; frame-ancestors 'self'; form-action 'none'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'none'; connect-src 'none'; worker-src 'none'; media-src 'none'; manifest-src 'none'"

// regexpMustCompile keeps the public files standard-library-only while making
// the method grammar declaration obvious at the call site.
func regexpMustCompile(pattern string) *regexp.Regexp { return regexp.MustCompile(pattern) }
