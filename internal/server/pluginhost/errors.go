// Package pluginhost runs trusted, compiled-in native plugins. Native code is
// not isolated; all I/O supplied by this host is capability checked.
package pluginhost

import "fmt"

// Error is a host diagnostic. Adapters return subscriber failures to the core;
// these plugin_* codes are not JSON-RPC client error codes.
type Error struct{ Code, Detail string }

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Detail) }

// CallbackBusy marks a bounded runtime-pool queue overflow. Core maps this
// structural signal to its public retryable busy error without importing the
// pluginhost package.
func (e *Error) CallbackBusy() bool { return e != nil && e.Code == ErrPluginBusy }
func failure(code, format string, args ...any) error {
	return &Error{code, fmt.Sprintf(format, args...)}
}

const (
	ErrManifest   = "plugin_manifest_invalid"
	ErrMethod     = "plugin_method_out_of_scope"
	ErrPatch      = "plugin_patch_out_of_scope"
	ErrRead       = "plugin_read_out_of_scope"
	ErrBudget     = "plugin_read_budget_exceeded"
	ErrHotWrite   = "plugin_host_write_in_sync_call"
	ErrReentrant  = "plugin_reentrant_write"
	ErrHotEgress  = "plugin_egress_in_hot_path"
	ErrEgress     = "plugin_egress_denied"
	ErrInactive   = "plugin_host_inactive"
	ErrPluginBusy = "plugin_runtime_pool_busy"
)
