// Package strata provides an HTTP client for the Strata context lake API.
//
// Zero external dependencies — uses only the standard library (net/http, encoding/json).
//
//	client := strata.NewClient("http://localhost:8432", nil)
//
//	// Ingest events
//	n, _ := client.Ingest(ctx, "my-app", []strata.Event{
//	    {"event_type": "user.signup", "user_id": "u1"},
//	})
//
//	// Query with SQL
//	rows, _ := client.Query(ctx, "SELECT * FROM episodic LIMIT 10")
//
//	// Semantic search
//	results, _ := client.Find(ctx, "billing issue", 5, nil)
//
//	// Agent state
//	_ = client.StateSet(ctx, "bot-1", "mood", "happy")
//	entry, _ := client.StateGet(ctx, "bot-1", "mood")
package strata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClientOptions configures the Strata client.
type ClientOptions struct {
	// APIKey for Bearer authentication. Empty means no auth.
	APIKey string
	// Timeout for HTTP requests (default: 30s).
	Timeout time.Duration
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
}

// Client is an HTTP client for the Strata context lake REST API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// Error is returned when the Strata API responds with an error.
type Error struct {
	Code      string
	Message   string
	RequestID string
	Status    int
}

func (e *Error) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("strata: %s (code=%s, status=%d, request_id=%s)", e.Message, e.Code, e.Status, e.RequestID)
	}
	return fmt.Sprintf("strata: %s (code=%s, status=%d)", e.Message, e.Code, e.Status)
}

// NewClient creates a new Strata client. Pass nil for opts to use defaults.
func NewClient(baseURL string, opts *ClientOptions) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	c := &Client{baseURL: baseURL}

	if opts != nil {
		c.apiKey = opts.APIKey
		if opts.HTTPClient != nil {
			c.httpClient = opts.HTTPClient
		} else {
			timeout := opts.Timeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			c.httpClient = &http.Client{Timeout: timeout}
		}
	} else {
		c.httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	return c
}

// ── Internal helpers ─────────────────────────────────────────────

func (c *Client) doRequest(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("strata: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("strata: create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("strata: do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("strata: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var ae apiError
		if json.Unmarshal(respBody, &ae) == nil && ae.Message != "" {
			return nil, resp.StatusCode, &Error{
				Code:      ae.Code,
				Message:   ae.Message,
				RequestID: ae.RequestID,
				Status:    resp.StatusCode,
			}
		}
		return nil, resp.StatusCode, &Error{
			Code:    "HTTP_ERROR",
			Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			Status:  resp.StatusCode,
		}
	}

	return respBody, resp.StatusCode, nil
}

// ── Health ───────────────────────────────────────────────────────

// Health checks server health.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, err
	}
	var r HealthResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode health: %w", err)
	}
	return &r, nil
}

// ── Query ────────────────────────────────────────────────────────

// Query executes a SQL query against the episodic store.
func (c *Client) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/query", QueryRequest{SQL: sql})
	if err != nil {
		return nil, err
	}
	var r QueryResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode query: %w", err)
	}
	return r.Rows, nil
}

// ── Ingest ───────────────────────────────────────────────────────

// Ingest ingests events into episodic memory. Returns the count of events ingested.
func (c *Client) Ingest(ctx context.Context, source string, events []Event) (int, error) {
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/ingest", IngestRequest{
		Source: source,
		Events: events,
	})
	if err != nil {
		return 0, err
	}
	var r IngestResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("strata: decode ingest: %w", err)
	}
	return r.Ingested, nil
}

// ── Search ───────────────────────────────────────────────────────

// Search performs semantic search by pre-computed vector.
func (c *Client) Search(ctx context.Context, vector []float64, k int, filters *SearchFilters) ([]SearchResult, error) {
	req := SearchRequest{Vector: vector, K: k, Filters: filters}
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/search", req)
	if err != nil {
		return nil, err
	}
	var r SearchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode search: %w", err)
	}
	return r.Results, nil
}

// Find performs semantic search by text (embed + search in one call).
func (c *Client) Find(ctx context.Context, text string, k int, filters *SearchFilters) ([]SearchResult, error) {
	req := FindRequest{Text: text, K: k, Filters: filters}
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/embed-and-search", req)
	if err != nil {
		return nil, err
	}
	var r SearchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode find: %w", err)
	}
	return r.Results, nil
}

// ── State ────────────────────────────────────────────────────────

// StateGet retrieves agent state. Returns nil, nil if not found.
func (c *Client) StateGet(ctx context.Context, agentID, key string) (*StateEntry, error) {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	body, status, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		if e, ok := err.(*Error); ok && e.Status == 404 {
			return nil, nil
		}
		return nil, err
	}
	if status == 404 {
		return nil, nil
	}
	var r StateEntry
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode state: %w", err)
	}
	return &r, nil
}

// StateSet sets agent state. Returns the new version number.
func (c *Client) StateSet(ctx context.Context, agentID, key string, value any) (int, error) {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	body, _, err := c.doRequest(ctx, http.MethodPut, path, value)
	if err != nil {
		return 0, err
	}
	var r StateSetResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("strata: decode state set: %w", err)
	}
	return r.Version, nil
}

// StateDelete deletes agent state.
func (c *Client) StateDelete(ctx context.Context, agentID, key string) error {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	_, _, err := c.doRequest(ctx, http.MethodDelete, path, nil)
	return err
}

// ── Schema ───────────────────────────────────────────────────────

// Sources lists all event sources.
func (c *Client) Sources(ctx context.Context) ([]string, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/schema/sources", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Sources []string `json:"sources"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode sources: %w", err)
	}
	return r.Sources, nil
}

// Agents lists all agent IDs.
func (c *Client) Agents(ctx context.Context) ([]string, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/schema/agents", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Agents []string `json:"agents"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode agents: %w", err)
	}
	return r.Agents, nil
}

// ── Admin ────────────────────────────────────────────────────────

// Backup triggers a backup of all stores.
func (c *Client) Backup(ctx context.Context) error {
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/admin/backup", struct{}{})
	return err
}

// EnforceRetention enforces the data retention policy.
func (c *Client) EnforceRetention(ctx context.Context) error {
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/admin/retention", struct{}{})
	return err
}

// ── Cluster ──────────────────────────────────────────────────────

// ClusterStatus returns the Raft cluster status.
func (c *Client) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/cluster/status", nil)
	if err != nil {
		return nil, err
	}
	var r ClusterStatus
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("strata: decode cluster status: %w", err)
	}
	return &r, nil
}
