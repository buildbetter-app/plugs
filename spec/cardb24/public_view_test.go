package cardb20

import (
	"encoding/json"
	"testing"
)

func staleDefinition(query string) map[string]any {
	return map[string]any{
		"filter":       map[string]any{"query": query},
		"fields":       []string{"id", "status", "project"},
		"sort":         []string{"id"},
		"budgetTokens": 1,
	}
}

func staleIDs(t *testing.T, got reply) (ids []string, cursor string) {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("stale query: %+v", got.Error)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatalf("stale result %s: %v", got.Result, err)
	}
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	return ids, page.NextCursor
}

func staleCount(t *testing.T, got reply) int {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("stale count: %+v", got.Error)
	}
	var result struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatalf("stale count result %s: %v", got.Result, err)
	}
	return result.Count
}
