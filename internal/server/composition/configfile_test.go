package composition

import (
	"strings"
	"testing"
)

func TestParseConfigSupportsFlatAndNestedAliases(t *testing.T) {
	config, err := ParseConfig([]byte("storage:\n  driver: sqlite\n  dsn: '/tmp/plug.db#durable'\nserver:\n  listen: 127.0.0.1:0\n  project: demo\n  ui: false\nauth:\n  mode: anonymous\n"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Driver != "sqlite" || config.DSN != "/tmp/plug.db#durable" || config.Address != "127.0.0.1:0" || config.Project != "demo" || config.UI || config.RollbackWindow || config.AuthMode != "anonymous" {
		t.Fatalf("parsed config=%+v", config)
	}
	window, err := ParseConfig([]byte("server:\n  rollback_window: true\n"))
	if err != nil || !window.RollbackWindow {
		t.Fatalf("rollback window config=%+v err=%v", window, err)
	}
	if disabled, err := ParseConfig([]byte("rollback_window: false\n")); err != nil || disabled.RollbackWindow {
		t.Fatalf("disabled rollback window config=%+v err=%v", disabled, err)
	}

	flat, err := ParseConfig([]byte("driver: memory\naddress: 127.0.0.1:0\nproject: local\nui: true\n"))
	if err != nil || flat.Driver != "memory" || flat.Project != "local" || !flat.UI {
		t.Fatalf("flat config=%+v err=%v", flat, err)
	}
	if flat.Ontology != nil {
		t.Fatalf("standalone YAML unexpectedly installed ontology source: %+v", flat.Ontology)
	}

	quoted, err := ParseConfig([]byte("driver: \"memory\"\naddress: 127.0.0.1:0\nproject: \"true\"\nui: false\n"))
	if err != nil || quoted.Driver != "memory" || quoted.Project != "true" {
		t.Fatalf("quoted config=%+v err=%v", quoted, err)
	}
}

func TestParseConfigRejectsUnknownDuplicateAndMalformedValues(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{"unknown", "unknown_value: true\n", "unsupported config key"},
		{"duplicate alias", "listen: 127.0.0.1:0\naddress: 127.0.0.1:1\n", "duplicate config key"},
		{"duplicate nested alias", "driver: sqlite\nstorage:\n  driver: sqlite\n", "duplicate config key"},
		{"duplicate section", "storage:\n  driver: memory\nstorage:\n  dsn: ignored\n", "duplicate config section"},
		{"empty section", "storage:\n", "config section"},
		{"bad bool", "ui: maybe\n", "must be true or false"},
		{"quoted bool", "ui: \"true\"\n", "must be true or false"},
		{"rollback bad bool", "rollback_window: maybe\n", "must be true or false"},
		{"rollback quoted bool", "rollback_window: \"true\"\n", "must be true or false"},
		{"bad indentation", "driver: memory\n  dsn: secret\n", "invalid indentation"},
		{"flow value", "storage: {driver: sqlite}\n", "unsupported config key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(test.data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
			if strings.Contains(err.Error(), "secret") || (test.name == "flow value" && strings.Contains(err.Error(), "sqlite")) {
				t.Fatalf("config value leaked in error: %v", err)
			}
		})
	}
}

func TestParseConfigRefusesUnsafeAnonymousListenerAndInvalidStorage(t *testing.T) {
	for _, data := range []string{
		"listen: 0.0.0.0:0\n",
		"listen: :0\n",
		"listen: example.invalid:0\n",
		"listen: 127.0.0.1:999999999999999999999999\n",
		"driver: memory\ndsn: secret\n",
		"driver: sqlite\n",
		"driver: postgres\n",
		"driver: unknown\n",
	} {
		_, err := ParseConfig([]byte(data))
		if err == nil {
			t.Fatalf("accepted invalid config %q", data)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("DSN leaked in error: %v", err)
		}
	}
}

func TestParseConfigSelectsOnlyProtectedInstalledOntologyIdentity(t *testing.T) {
	for _, data := range []string{
		"ontology:\n  source: ontology/b03-runtime\n",
		"ontology.source: ontology/b03-runtime\n",
	} {
		config, err := ParseConfig([]byte(data))
		if err != nil || config.Ontology == nil || config.Ontology.Source == nil {
			t.Fatalf("installed source config=%+v err=%v", config, err)
		}
		ref, err := config.Ontology.Source(config.Project)
		if err != nil || ref.ID != "ontology/b03-runtime" || !ref.Branch.IsZero() {
			t.Fatalf("installed source ref=%+v err=%v", ref, err)
		}
		if config.Ontology.Authorizer != nil || config.Ontology.ValidatingPre != nil || config.Ontology.EventContext != nil {
			t.Fatal("configuration manufactured trusted plugin capabilities")
		}
	}
	for _, data := range []string{
		"ontology:\n  source: bead/not-ontology\n",
		"ontology.source: ''\n",
		"ontology.source: 'ontology/with space'\n",
		"ontology.source: true\n",
		"ontology.source: ontology/one\nontology:\n  source: ontology/two\n",
		"ontology:\n  authorizer: allow\n",
	} {
		if _, err := ParseConfig([]byte(data)); err == nil {
			t.Fatalf("accepted invalid installed source configuration %q", data)
		}
	}
}
