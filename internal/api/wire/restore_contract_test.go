package wire

import (
	"strings"
	"testing"
)

func TestRestoreImportCommentContract(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	comment, err := c.resolve("#/components/schemas/Comment")
	if err != nil {
		t.Fatal(err)
	}
	check := func(name, raw string, valid bool) {
		t.Run(name, func(t *testing.T) {
			value, err := DecodeJSON([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			err = c.validate(comment, value, "comment", false, 0)
			if (err == nil) != valid {
				t.Fatalf("valid=%v: %v", valid, err)
			}
		})
	}
	principal := `"principal":{"kind":"user","id":"restore-admin"}`
	check("legacy shape remains valid", `{"body":"old",`+principal+`,"time":"2026-10-30T19:05:00Z"}`, true)
	check("imported shape", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"Ada","sourceTime":"2024-01-02T03:04:05Z"}`, true)
	check("imported source author omitted", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z"}`, true)
	check("import origin requires source time", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import"}`, false)
	check("wrong origin refused", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"manual","sourceTime":"2024-01-02T03:04:05Z"}`, false)
	check("unknown trusted field refused", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z","actor":"forged"}`, false)
	check("source author max scalar bound", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"`+strings.Repeat("a", 257)+`","sourceTime":"2024-01-02T03:04:05Z"}`, false)
	check("source author 256 scalars", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"`+strings.Repeat("a", 256)+`","sourceTime":"2024-01-02T03:04:05Z"}`, true)

	result, _ := c.Descriptor("bead.get")
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"Ada","sourceTime":"2024-01-02T03:04:05Z"}]}`)); err != nil {
		t.Fatalf("bead.get must expose optional typed comments: %v", err)
	}
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"old",`+principal+`,"time":"2026-10-30T19:05:00Z"}]}`)); err != nil {
		t.Fatalf("bead.get must retain legacy comments: %v", err)
	}
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"x",`+principal+`,"time":"2026-10-30T19:05:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z","principalOverride":"attacker"}]}`)); err == nil {
		t.Fatal("bead.get accepted an unknown imported-comment field")
	}

	contract, ok := c.root["x-restore-import-contract"].(map[string]any)
	if !ok {
		t.Fatal("missing restore-import contract extension")
	}
	ordinary, ok := contract["ordinaryInputs"].(map[string]any)
	if !ok || ordinary["commentOp"] != "closed to kind, target and body" {
		t.Fatal("ordinary comment input boundary is not pinned")
	}
	trusted, ok := contract["trustedAdmission"].(map[string]any)
	if !ok || trusted["hostPrivate"] != true {
		t.Fatal("prepared import profile must remain host-private")
	}
	allocatorContract, ok := contract["allocator"].(map[string]any)
	if !ok || allocatorContract["prefixBinding"] != "targetAllocator.prefix must equal sourceAllocator.prefix; mismatch refuses before staging." || allocatorContract["sourcePersistence"] != "Persist sourceAllocator unchanged in the restore receipt and project metadata key plug.restore.sourceAllocator." {
		t.Fatal("allocator provenance/prefix binding contract drift")
	}
}

func TestAdminRestoreBeginAcceptsOptionalHistoryBindings(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := c.Descriptor("admin.restore.begin")
	if !ok {
		t.Fatal("missing admin.restore.begin descriptor")
	}
	prefix := `{"manifest":{},"humanPresence":{"mode":"presence_check","challenge":"restore","proof":"restore"}`
	valid := prefix + `,"historyDigest":"` + strings.Repeat("a", 64) + `","historyTime":"2026-11-20T12:34:56Z"}`
	if err := c.ValidateParams(descriptor, []byte(valid)); err != nil {
		t.Fatalf("optional history bindings rejected: %v", err)
	}
	for _, raw := range []string{
		prefix + `,"historyDigest":"short"}`,
		prefix + `,"historyTime":42}`,
		prefix + `,"historyDigest":42}`,
	} {
		if err := c.ValidateParams(descriptor, []byte(raw)); err == nil {
			t.Fatalf("invalid optional history binding accepted: %s", raw)
		}
	}
}

func TestRestoreImportAllocatorAndProfileContract(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	validate := func(ref, raw string, valid bool) {
		t.Helper()
		schema, err := c.resolve(ref)
		if err != nil {
			t.Fatal(err)
		}
		value, err := DecodeJSON([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		err = c.validate(schema, value, ref, false, 0)
		if (err == nil) != valid {
			t.Fatalf("%s valid=%v: %v", ref, valid, err)
		}
	}
	source := `{"prefix":"pl","id_mode":"hash","hash_length":32,"last_id":0,"child_counters":{"pl-a":2}}`
	target := `{"prefix":"pl","id_mode":"random","random_bytes":16,"suffix_encoding":"lower-hex","top_level_shape":"prefix-random","child_shape":"parent-random"}`
	validate("#/components/schemas/SourceAllocatorProvenance", source, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":1,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":64,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":65,"last_id":0}`, false)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":0,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":64,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":65,"last_id":0}`, false)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","last_id":0}`, false)
	validate("#/components/schemas/TargetRandomAllocator", target, true)
	validate("#/components/schemas/TargetRandomAllocator", `{"prefix":"pl","id_mode":"hash","random_bytes":16,"suffix_encoding":"lower-hex","top_level_shape":"prefix-random","child_shape":"parent-random"}`, false)
	validate("#/components/schemas/RestoreAllocatorProfile", `{"sourceAllocator":`+source+`,"targetAllocator":`+target+`}`, false)
	hashTarget := `{"prefix":"pl","id_mode":"hash","hash_length":8,"min_hash_length":3,"max_hash_length":8,"max_collision_prob":0.25,"last_id":0,"child_counters":{"pl-a":2}}`
	validate("#/components/schemas/TargetHashAllocator", hashTarget, true)
	validate("#/components/schemas/RestoreAllocatorProfile", `{"sourceAllocator":`+source+`,"targetAllocator":`+hashTarget+`}`, true)
	validate("#/components/schemas/TargetHashAllocator", strings.Replace(hashTarget, `"min_hash_length":3`, `"min_hash_length":2`, 1), false)

	admission := `{"mode":"authenticated","actor":{"kind":"user","id":"admin"},"scope":"","authority":"restore-admin","admissionDigest":"` + strings.Repeat("a", 64) + `"}`
	profile := `{"admission":` + admission + `,"sourceStoreId":"store-1","method":"offline.restore.import","idempotencyKey":"0123456789abcdef","archiveDigest":"` + strings.Repeat("b", 64) + `","derivedDigest":"` + strings.Repeat("c", 64) + `"}`
	validate("#/components/schemas/RestoreImportAdmission", admission, true)
	validate("#/components/schemas/RestoreImportProfile", profile, true)
	validate("#/components/schemas/RestoreImportProfile", `{"admission":`+admission+`,"sourceStoreId":"store-1","method":"admin.restore.finish","idempotencyKey":"0123456789abcdef","archiveDigest":"`+strings.Repeat("b", 64)+`","derivedDigest":"`+strings.Repeat("c", 64)+`"}`, false)
	validate("#/components/schemas/RestoreImportAdmission", `{"mode":"authenticated","actor":{"kind":"user","id":"admin"},"scope":"","authority":"trusted-human-presence","admissionDigest":"`+strings.Repeat("a", 64)+`"}`, false)

	beadGet, _ := c.Descriptor("bead.get")
	if err = c.ValidateResult(beadGet, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[]}`)); err != nil {
		t.Fatal("optional comments array must be valid", err)
	}
}
