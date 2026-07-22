import { EcphoriaError } from "./errors.js";
/**
 * Ecphoria client — fetch-based HTTP client for the Ecphoria agentic memory platform API.
 *
 * Zero runtime dependencies. Uses the global `fetch` API (Node 18+, Deno, Bun, browsers).
 *
 * @example
 * ```ts
 * const client = new EcphoriaClient({ url: "http://localhost:8432" });
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
export class EcphoriaClient {
    baseUrl;
    headers;
    timeout;
    constructor(options = {}) {
        this.baseUrl = (options.url ?? "http://localhost:8432").replace(/\/+$/, "");
        this.headers = { "Content-Type": "application/json" };
        if (options.apiKey) {
            this.headers["Authorization"] = `Bearer ${options.apiKey}`;
        }
        this.timeout = options.timeout ?? 30_000;
    }
    // ── Internal helpers ─────────────────────────────────────────────
    async request(method, path, body) {
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
                let apiErr;
                try {
                    const json = await resp.json();
                    if (json && typeof json === "object" && "message" in json) {
                        apiErr = json;
                    }
                }
                catch {
                    // not JSON
                }
                if (apiErr) {
                    throw EcphoriaError.fromApiError(apiErr, resp.status);
                }
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return (await resp.json());
        }
        finally {
            clearTimeout(timer);
        }
    }
    async get(path) {
        return this.request("GET", path);
    }
    async post(path, body) {
        return this.request("POST", path, body);
    }
    async put(path, body) {
        return this.request("PUT", path, body);
    }
    async del(path) {
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
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
        }
        finally {
            clearTimeout(timer);
        }
    }
    // ── Health ───────────────────────────────────────────────────────
    /** Check server health. */
    async health() {
        return this.get("/health");
    }
    // ── Query ────────────────────────────────────────────────────────
    /** Execute a SQL query against the episodic store. Returns row dicts. */
    async query(sql) {
        const data = await this.post("/api/v1/query", { sql });
        return data.rows ?? [];
    }
    // ── Ingest ───────────────────────────────────────────────────────
    /** Ingest events into episodic memory. Returns the count of events ingested. */
    async ingest(source, events) {
        const data = await this.post("/api/v1/ingest", {
            source,
            events,
        });
        return data.ingested ?? 0;
    }
    // ── Search ───────────────────────────────────────────────────────
    /** Semantic search by pre-computed vector. */
    async search(vector, options = {}) {
        const body = { vector, k: options.k ?? 5 };
        if (options.filters)
            body.filters = options.filters;
        const data = await this.post("/api/v1/search", body);
        return data.results ?? [];
    }
    /** Semantic search by natural language text (embed + search in one call). */
    async find(text, options = {}) {
        const body = { text, k: options.k ?? 5 };
        if (options.filters)
            body.filters = options.filters;
        const data = await this.post("/api/v1/embed-and-search", body);
        return data.results ?? [];
    }
    // ── State ────────────────────────────────────────────────────────
    /** Get agent state. Returns null if not found. */
    async stateGet(agentId, key) {
        const url = `${this.baseUrl}/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        try {
            const resp = await fetch(url, {
                headers: this.headers,
                signal: controller.signal,
            });
            if (resp.status === 404)
                return null;
            if (!resp.ok) {
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return (await resp.json());
        }
        finally {
            clearTimeout(timer);
        }
    }
    /** Set agent state. Returns the new version number. */
    async stateSet(agentId, key, value) {
        const data = await this.put(`/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`, value);
        return data.version ?? 0;
    }
    /** Delete agent state. */
    async stateDelete(agentId, key) {
        await this.del(`/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`);
    }
    // ── Schema ───────────────────────────────────────────────────────
    /** List all event sources. */
    async sources() {
        const data = await this.get("/api/v1/schema/sources");
        return data.sources ?? [];
    }
    /** List all agent IDs. */
    async agents() {
        const data = await this.get("/api/v1/schema/agents");
        return data.agents ?? [];
    }
    // ── Admin ────────────────────────────────────────────────────────
    /** Trigger a backup of all stores. */
    async backup() {
        return this.post("/api/v1/admin/backup", {});
    }
    /** Enforce data retention policy. */
    async enforceRetention() {
        return this.post("/api/v1/admin/retention", {});
    }
    // ── Memory (cognition layer) ─────────────────────────────────────
    /** Add a memory through the cognition pipeline (dedup / contradiction / importance). */
    async memoryAdd(content, opts = {}) {
        return this.post("/api/v1/memories", { content, ...opts });
    }
    /** Hybrid (BM25 + vector) search over a scope's memories. Returns ranked hits. */
    async memorySearch(query, opts = {}) {
        const { k = 5, ...scope } = opts;
        const data = await this.post("/api/v1/memories/search", { query, k, ...scope });
        return data.results ?? [];
    }
    /** List active memories in a scope. */
    async memoryList(opts = {}) {
        const params = new URLSearchParams();
        params.set("limit", String(opts.limit ?? 50));
        for (const key of ["tenant_id", "user_id", "agent_id", "session_id"]) {
            const v = opts[key];
            if (v !== undefined)
                params.set(key, v);
        }
        const data = await this.get(`/api/v1/memories?${params.toString()}`);
        return data.memories ?? [];
    }
    /** Get a memory by id. Returns null if not found (or not in your tenant). */
    async memoryGet(id) {
        const url = `${this.baseUrl}/api/v1/memories/${encodeURIComponent(id)}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        try {
            const resp = await fetch(url, { headers: this.headers, signal: controller.signal });
            if (resp.status === 404)
                return null;
            if (!resp.ok) {
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return (await resp.json());
        }
        finally {
            clearTimeout(timer);
        }
    }
    /** Bi-temporal history for a memory's subject (oldest first). */
    async memoryHistory(id) {
        const data = await this.get(`/api/v1/memories/${encodeURIComponent(id)}/history`);
        return data.history ?? [];
    }
    /** Delete a memory by id. Returns false if it didn't exist (or not in your tenant). */
    async memoryDelete(id) {
        const url = `${this.baseUrl}/api/v1/memories/${encodeURIComponent(id)}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        try {
            const resp = await fetch(url, {
                method: "DELETE",
                headers: this.headers,
                signal: controller.signal,
            });
            if (resp.status === 404)
                return false;
            if (!resp.ok) {
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return true;
        }
        finally {
            clearTimeout(timer);
        }
    }
    // ── Cognition (provenance / feedback / contradictions) ───────────
    /** A memory's source events + supersession chain (the audit trail behind a fact). */
    async memoryProvenance(id) {
        return this.get(`/api/v1/memories/${encodeURIComponent(id)}/provenance`);
    }
    /** Feedback so ranking learns: 'helpful' reinforces, 'wrong'/'obsolete' retires. */
    async memoryFeedback(id, verdict) {
        return this.post(`/api/v1/memories/${encodeURIComponent(id)}/feedback`, { verdict });
    }
    /** Subjects with more than one active memory (the review queue). */
    async memoryContradictions(userId) {
        const q = userId ? `?user_id=${encodeURIComponent(userId)}` : "";
        const data = await this.get(`/api/v1/memories/contradictions${q}`);
        return data.contradictions ?? [];
    }
    /** Resolve a contradiction: keep `keepId`, supersede the rest for `subject`. */
    async memoryResolveContradiction(subject, keepId, userId) {
        const body = { subject, keep_id: keepId };
        if (userId)
            body.user_id = userId;
        return this.post("/api/v1/memories/contradictions/resolve", body);
    }
    // ── Knowledge graph ──────────────────────────────────────────────
    /** Add a graph edge (src -[relation]-> dst). supersede closes the prior (src, relation). */
    async memoryLink(src, relation, dst, supersede = false) {
        return this.post("/api/v1/memories/link", { src, relation, dst, supersede });
    }
    /** Edges around an entity (depth>1 expands the subgraph). */
    async graphNeighbors(entity, depth = 1, limit = 50) {
        const p = new URLSearchParams({
            entity,
            depth: String(depth),
            limit: String(limit),
        });
        const data = await this.get(`/api/v1/memories/graph?${p.toString()}`);
        return data.edges ?? [];
    }
    /** All knowledge-graph edges (bulk view). */
    async graphEdges(limit = 10000) {
        const data = await this.get(`/api/v1/memories/edges?limit=${limit}`);
        return data.edges ?? [];
    }
    /** Degree + PageRank per node, optionally as-of a time. */
    async graphCentrality(asOf, limit) {
        const p = new URLSearchParams();
        if (asOf)
            p.set("as_of", asOf);
        if (limit)
            p.set("limit", String(limit));
        const qs = p.toString();
        const data = await this.get(`/api/v1/memories/graph/centrality${qs ? `?${qs}` : ""}`);
        return data.nodes ?? [];
    }
    /** Shortest directed path between two entities (null if unreachable). */
    async graphPath(src, dst, asOf) {
        const p = new URLSearchParams({ src, dst });
        if (asOf)
            p.set("as_of", asOf);
        const data = await this.get(`/api/v1/memories/graph/path?${p.toString()}`);
        return data.path ?? null;
    }
    /** Community detection (connected clusters), optionally as-of a time. */
    async graphCommunities(asOf) {
        const qs = asOf ? `?as_of=${encodeURIComponent(asOf)}` : "";
        const data = await this.get(`/api/v1/memories/graph/communities${qs}`);
        return data.communities ?? [];
    }
    // ── Templates ────────────────────────────────────────────────────
    /** Built-in memory templates. */
    async memoryTemplates() {
        const data = await this.get("/api/v1/memory-templates");
        return data.templates ?? [];
    }
    /** Create a memory from a template + field values. */
    async memoryFromTemplate(template, fields, userId) {
        const body = { template, fields };
        if (userId)
            body.user_id = userId;
        return this.post("/api/v1/memories/from-template", body);
    }
    // ── Attachments (multimodal) ─────────────────────────────────────
    /** Upload a blob (image/PDF/audio). Optional caption stores a searchable memory. */
    async attachmentUpload(data, opts = {}) {
        const p = new URLSearchParams();
        if (opts.filename)
            p.set("filename", opts.filename);
        if (opts.memoryId)
            p.set("memory_id", opts.memoryId);
        if (opts.caption)
            p.set("caption", opts.caption);
        const qs = p.toString();
        const url = `${this.baseUrl}/api/v1/attachments${qs ? `?${qs}` : ""}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        try {
            const resp = await fetch(url, {
                method: "POST",
                headers: {
                    ...this.headers,
                    "Content-Type": opts.contentType ?? "application/octet-stream",
                },
                // Uint8Array is a valid BodyInit at runtime; the DOM lib's type is narrower here.
                body: data,
                signal: controller.signal,
            });
            if (!resp.ok) {
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return (await resp.json());
        }
        finally {
            clearTimeout(timer);
        }
    }
    /** List attachments (optionally for one memory). */
    async attachmentList(memoryId) {
        const qs = memoryId ? `?memory_id=${encodeURIComponent(memoryId)}` : "";
        const data = await this.get(`/api/v1/attachments${qs}`);
        return data.attachments ?? [];
    }
    /** Delete an attachment. Returns false if it didn't exist. */
    async attachmentDelete(id) {
        const url = `${this.baseUrl}/api/v1/attachments/${encodeURIComponent(id)}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        try {
            const resp = await fetch(url, {
                method: "DELETE",
                headers: this.headers,
                signal: controller.signal,
            });
            if (resp.status === 404)
                return false;
            if (!resp.ok) {
                throw new EcphoriaError(`HTTP ${resp.status}: ${resp.statusText}`, "HTTP_ERROR", resp.status);
            }
            return true;
        }
        finally {
            clearTimeout(timer);
        }
    }
    // ── Admin / sessions extras ──────────────────────────────────────
    /** Re-embed active memories with the current provider (after a model change). */
    async memoryReembed(limit = 1000) {
        return this.post("/api/v1/admin/memory/reembed", { limit });
    }
    /** Consolidate a session's events into memory. */
    async sessionDistill(sessionId) {
        return this.post(`/api/v1/sessions/${encodeURIComponent(sessionId)}/distill`, {});
    }
    // ── Sessions ─────────────────────────────────────────────────────
    /** Start a conversation session. */
    async sessionStart(sessionId, agentId, opts = {}) {
        const body = {
            session_id: sessionId,
            agent_id: agentId,
        };
        if (opts.parentSessionId)
            body.parent_session_id = opts.parentSessionId;
        if (opts.metadata)
            body.metadata = opts.metadata;
        return this.post("/api/v1/sessions", body);
    }
    /** End a session, optionally attaching a summary. */
    async sessionEnd(sessionId, summary) {
        return this.post(`/api/v1/sessions/${encodeURIComponent(sessionId)}/end`, summary ? { summary } : {});
    }
    /** Recall all events recorded in a session. */
    async sessionRecall(sessionId) {
        const data = await this.get(`/api/v1/sessions/${encodeURIComponent(sessionId)}/recall`);
        return data.events ?? [];
    }
    // ── Agentic platform (runs, agents, triggers, tools) ─────────────
    /** Create a durable agent/workflow run. Returns the run. */
    async runCreate(opts = {}) {
        const body = {};
        if (opts.agentId !== undefined)
            body.agent_id = opts.agentId;
        if (opts.input !== undefined)
            body.input = opts.input;
        if (opts.parentRunId !== undefined)
            body.parent_run_id = opts.parentRunId;
        const data = await this.post("/api/v1/runs", body);
        return data.run ?? {};
    }
    /** Get a run by id. */
    async runGet(id) {
        const data = await this.get(`/api/v1/runs/${encodeURIComponent(id)}`);
        return data.run ?? null;
    }
    /** List runs (newest first), optionally filtered by status. */
    async runList(opts = {}) {
        const params = new URLSearchParams();
        params.set("limit", String(opts.limit ?? 50));
        if (opts.status)
            params.set("status", opts.status);
        const data = await this.get(`/api/v1/runs?${params.toString()}`);
        return data.runs ?? [];
    }
    /** Full step trace of a run (LLM/tool/HITL steps). */
    async runTrace(id) {
        const data = await this.get(`/api/v1/runs/${encodeURIComponent(id)}/trace`);
        return data.steps ?? [];
    }
    /** Cancel a run. */
    async runCancel(id) {
        return this.post(`/api/v1/runs/${encodeURIComponent(id)}/cancel`, {});
    }
    /** Run an agent end-to-end (durable LLM↔tool loop). Returns the resulting run. */
    async runAgent(agentId, question, opts = {}) {
        const body = { agent_id: agentId, question };
        if (opts.maxTurns !== undefined)
            body.max_turns = opts.maxTurns;
        const data = await this.post("/api/v1/agents/run", body);
        return data.run ?? {};
    }
    /** Approve or reject a run awaiting approval (HITL). */
    async runApprove(id, approve = true) {
        return this.post(`/api/v1/runs/${encodeURIComponent(id)}/approve`, { approve });
    }
    /** Resume an approved run (durable resume after HITL). */
    async runResume(id) {
        const data = await this.post(`/api/v1/runs/${encodeURIComponent(id)}/resume`, {});
        return data.run ?? {};
    }
    /** Register an event trigger: matching events start a run of `agentId`. */
    async triggerRegister(name, agentId, opts = {}) {
        return this.post("/api/v1/triggers", {
            name,
            agent_id: agentId,
            source: opts.source ?? "*",
            event_type: opts.eventType ?? "*",
        });
    }
    /** List registered event triggers. */
    async triggerList() {
        const data = await this.get("/api/v1/triggers");
        return data.triggers ?? [];
    }
    /** Register a downstream MCP tool server. */
    async toolRegister(name, url) {
        return this.post("/api/v1/tools", { name, url });
    }
    /** List registered downstream MCP tool servers. */
    async toolList() {
        const data = await this.get("/api/v1/tools");
        return data.servers ?? [];
    }
    /** Invoke a tool on a registered downstream MCP server. */
    async toolCall(server, tool, args = {}) {
        const data = await this.post(`/api/v1/tools/${encodeURIComponent(server)}/call`, { tool, arguments: args });
        return data.result;
    }
    // ── Cluster ──────────────────────────────────────────────────────
    /** Get Raft cluster status. */
    async clusterStatus() {
        return this.get("/cluster/status");
    }
}
//# sourceMappingURL=client.js.map