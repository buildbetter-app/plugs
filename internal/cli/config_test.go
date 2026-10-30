package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFileConfigAcceptsSQLiteAndServerForms(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		server  string
		storage StorageConfig
	}{
		{name: "nested sqlite", text: "storage:\n  driver: sqlite\n  path: .plug/data.db\n", storage: StorageConfig{Driver: "sqlite", DSN: ".plug/data.db"}},
		{name: "scalar sqlite", text: "storage: sqlite\n", storage: StorageConfig{Driver: "sqlite", DSN: ".plug/plug.db"}},
		{name: "shorthand path", text: "path: custom.db\n", storage: StorageConfig{Driver: "sqlite", DSN: "custom.db"}},
		{name: "server", text: "server: https://plug.example/rpc/v1\n", server: "https://plug.example/rpc/v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFileConfig(tt.text)
			if err != nil {
				t.Fatal(err)
			}
			if got.server != tt.server || got.storage != tt.storage || got.serverSet != (tt.server != "") || got.storageSet != (tt.server == "") {
				t.Fatalf("config=%+v, want server=%q storage=%+v", got, tt.server, tt.storage)
			}
		})
	}
}

func TestParseFileConfigRefusesAmbiguousOrUnsupportedValues(t *testing.T) {
	tests := []string{
		"server: https://plug.example\nstorage: sqlite\n",
		"storage:\n  driver: sqlite\n  driver: memory\n",
		"storage:\n  driver: memory\n",
		"storage:\n  driver: postgres\n",
		"storage:\n  unknown: value\n",
		"storage:\n    driver: sqlite\n",
		"storage: [sqlite]\n",
		"unsupported: value\n",
		"storage:\n  path: null\n",
		"storage:\n  path: file:store.db\n",
		"storage:\n\tdriver: sqlite\n",
	}
	for _, text := range tests {
		t.Run(strings.ReplaceAll(strings.TrimSpace(text), "\n", "/"), func(t *testing.T) {
			if _, err := parseFileConfig(text); err == nil {
				t.Fatalf("accepted unsupported configuration %q", text)
			}
		})
	}
}

func TestSQLiteFactoryCreatesParentsOnlyAfterConfigurationValidation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "one", "two", "plug.db")
	if _, _, err := SQLiteFactory(context.Background(), StorageConfig{Driver: "postgres", DSN: path}); err == nil {
		t.Fatal("unsupported driver created a store")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("unsupported driver created parent directory: %v", err)
	}
	store, closeStore, err := SQLiteFactory(context.Background(), StorageConfig{Driver: "sqlite", DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	if store == nil || closeStore == nil {
		t.Fatal("SQLite factory returned incomplete store")
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("parent directory was not private: info=%v err=%v", info, err)
	}
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("SQLite file was not created: %v", err)
	}
}
