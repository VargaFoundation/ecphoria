import { StrataError } from "./errors.js";
import type {
  BackupResponse,
  ClusterStatus,
  Event,
  FindRequest,
  HealthResponse,
  IngestResponse,
  QueryResponse,
  RetentionResponse,
  SearchFilters,
  SearchRequest,
  SearchResult,
  StateEntry,
  StateSetResponse,
  StrataApiError,
  StrataClientOptions,
} from "./types.js";

/**
 * Strata client — fetch-based HTTP client for the Strata context lake API.
 *
 * Zero runtime dependencies. Uses the global `fetch` API (Node 18+, Deno, Bun, browsers).
 *
 * @example
 * ```ts
 * const client = new StrataClient({ url: "http://localhost:8432" });
 *
 * // Ingest events
 * const count = await client.ingest("my-app", [
 *   { event_type: "user.signup", user_id: "u1" },
 * ]);
 *
 * // Query with SQL
 * const rows = await client.query("SELECT * FROM episodic LIMIT 10");
 *
 * // Semantic search by text
 * const results = await client.find("frustrated customer", { k: 5 });
 *
 * // Agent state
 * await client.stateSet("bot-1", "mood", "happy");
 * const entry = await client.stateGet("bot-1", "mood");
 * ```
 */
export class StrataClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeout: number;

  constructor(options: StrataClientOptions = {}) {
    this.baseUrl = (options.url ?? "http://localhost:8432").replace(/\/+$/, "");
    this.headers = { "Content-Type": "application/json" };
    if (options.apiKey) {
      this.headers["Authorization"] = `Bearer ${options.apiKey}`;
    }
    this.timeout = options.timeout ?? 30_000;
  }

  // ── Internal helpers ─────────────────────────────────────────────

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    try {
      const resp = await fetch(url, {
        method,
        headers: this.headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      if (!resp.ok) {
        let apiErr: StrataApiError | undefined;
        try {
          const json = await resp.json();
          if (json && typeof json === "object" && "message" in json) {
            apiErr = json as StrataApiError;
          }
        } catch {
          // not JSON
        }
        if (apiErr) {
          throw StrataError.fromApiError(apiErr, resp.status);
        }
        throw new StrataError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }

      return (await resp.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  private async get<T>(path: string): Promise<T> {
    return this.request<T>("GET", path);
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("POST", path, body);
  }

  private async put<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("PUT", path, body);
  }

  private async del(path: string): Promise<void> {
    const url = `${this.baseUrl}${path}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "DELETE",
        headers: this.headers,
        signal: controller.signal,
      });
      if (!resp.ok) {
        throw new StrataError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
    } finally {
      clearTimeout(timer);
    }
  }

  // ── Health ───────────────────────────────────────────────────────

  /** Check server health. */
  async health(): Promise<HealthResponse> {
    return this.get<HealthResponse>("/health");
  }

  // ── Query ────────────────────────────────────────────────────────

  /** Execute a SQL query against the episodic store. Returns row dicts. */
  async query(sql: string): Promise<Record<string, unknown>[]> {
    const data = await this.post<QueryResponse>("/api/v1/query", { sql });
    return data.rows ?? [];
  }

  // ── Ingest ───────────────────────────────────────────────────────

  /** Ingest events into episodic memory. Returns the count of events ingested. */
  async ingest(source: string, events: Event[]): Promise<number> {
    const data = await this.post<IngestResponse>("/api/v1/ingest", {
      source,
      events,
    });
    return data.ingested ?? 0;
  }

  // ── Search ───────────────────────────────────────────────────────

  /** Semantic search by pre-computed vector. */
  async search(
    vector: number[],
    options: { k?: number; filters?: SearchFilters } = {},
  ): Promise<SearchResult[]> {
    const body: SearchRequest = { vector, k: options.k ?? 5 };
    if (options.filters) body.filters = options.filters;
    const data = await this.post<{ results: SearchResult[] }>("/api/v1/search", body);
    return data.results ?? [];
  }

  /** Semantic search by natural language text (embed + search in one call). */
  async find(
    text: string,
    options: { k?: number; filters?: SearchFilters } = {},
  ): Promise<SearchResult[]> {
    const body: FindRequest = { text, k: options.k ?? 5 };
    if (options.filters) body.filters = options.filters;
    const data = await this.post<{ results: SearchResult[] }>(
      "/api/v1/embed-and-search",
      body,
    );
    return data.results ?? [];
  }

  // ── State ────────────────────────────────────────────────────────

  /** Get agent state. Returns null if not found. */
  async stateGet(agentId: string, key: string): Promise<StateEntry | null> {
    const url = `${this.baseUrl}/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        headers: this.headers,
        signal: controller.signal,
      });
      if (resp.status === 404) return null;
      if (!resp.ok) {
        throw new StrataError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return (await resp.json()) as StateEntry;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Set agent state. Returns the new version number. */
  async stateSet(agentId: string, key: string, value: unknown): Promise<number> {
    const data = await this.put<StateSetResponse>(
      `/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`,
      value,
    );
    return data.version ?? 0;
  }

  /** Delete agent state. */
  async stateDelete(agentId: string, key: string): Promise<void> {
    await this.del(
      `/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`,
    );
  }

  // ── Schema ───────────────────────────────────────────────────────

  /** List all event sources. */
  async sources(): Promise<string[]> {
    const data = await this.get<{ sources: string[] }>("/api/v1/schema/sources");
    return data.sources ?? [];
  }

  /** List all agent IDs. */
  async agents(): Promise<string[]> {
    const data = await this.get<{ agents: string[] }>("/api/v1/schema/agents");
    return data.agents ?? [];
  }

  // ── Admin ────────────────────────────────────────────────────────

  /** Trigger a backup of all stores. */
  async backup(): Promise<BackupResponse> {
    return this.post<BackupResponse>("/api/v1/admin/backup", {});
  }

  /** Enforce data retention policy. */
  async enforceRetention(): Promise<RetentionResponse> {
    return this.post<RetentionResponse>("/api/v1/admin/retention", {});
  }

  // ── Cluster ──────────────────────────────────────────────────────

  /** Get Raft cluster status. */
  async clusterStatus(): Promise<ClusterStatus> {
    return this.get<ClusterStatus>("/cluster/status");
  }
}
