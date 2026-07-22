package ecphoria

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func newTestClient(handler http.HandlerFunc) (*Client, *httptest.Server) {
	srv := httptest.NewServer(handler)
	return NewClient(srv.URL, nil), srv
}

func strPtr(s string) *string { return &s }

func TestMemoryUpdate(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	c, srv := newTestClient(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1","content":"corrected"}`))
	})
	defer srv.Close()

	content := "corrected"
	imp := 0.9
	out, err := c.MemoryUpdate(context.Background(), "m1", MemoryPatch{Content: &content, Importance: &imp})
	if err != nil {
		t.Fatalf("MemoryUpdate: %v", err)
	}
	if gotMethod != http.MethodPatch {
		t.Errorf("method = %s, want PATCH", gotMethod)
	}
	if gotPath != "/api/v1/memories/m1" {
		t.Errorf("path = %s", gotPath)
	}
	if gotBody["content"] != "corrected" {
		t.Errorf("body content = %v", gotBody["content"])
	}
	if out["content"] != "corrected" {
		t.Errorf("out content = %v", out["content"])
	}
}

func TestMemoryUpdateNotFound(t *testing.T) {
	c, srv := newTestClient(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	})
	defer srv.Close()
	out, err := c.MemoryUpdate(context.Background(), "missing", MemoryPatch{Content: strPtr("x")})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Errorf("want nil, got %v", out)
	}
}

func TestMemoryListWithFilters(t *testing.T) {
	var q url.Values
	c, srv := newTestClient(func(w http.ResponseWriter, r *http.Request) {
		q = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"memories":[],"count":0}`))
	})
	defer srv.Close()
	mi := 0.5
	_, err := c.MemoryListWith(context.Background(), MemoryListOptions{
		Limit:  10,
		Offset: 5,
		Scope:  MemoryScope{UserID: "alice"},
		Filter: MemoryFilter{MemType: "semantic", MinImportance: &mi, MetadataKey: "tag", MetadataValue: "vip"},
	})
	if err != nil {
		t.Fatalf("MemoryListWith: %v", err)
	}
	want := map[string]string{
		"limit":          "10",
		"offset":         "5",
		"user_id":        "alice",
		"mem_type":       "semantic",
		"min_importance": "0.5",
		"metadata_key":   "tag",
		"metadata_value": "vip",
	}
	for k, v := range want {
		if got := q.Get(k); got != v {
			t.Errorf("query[%s] = %q, want %q", k, got, v)
		}
	}
}

func TestMemoryScopes(t *testing.T) {
	c, srv := newTestClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/schema/memory-scopes" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"count":1,"scopes":[{"user_id":"alice","count":3}]}`))
	})
	defer srv.Close()
	scopes, err := c.MemoryScopes(context.Background())
	if err != nil {
		t.Fatalf("MemoryScopes: %v", err)
	}
	if len(scopes) != 1 || scopes[0]["user_id"] != "alice" {
		t.Errorf("scopes = %v", scopes)
	}
}
