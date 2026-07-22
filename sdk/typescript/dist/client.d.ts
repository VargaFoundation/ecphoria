import type { BackupResponse, ClusterStatus, Event, HealthResponse, MemoryScope, RetentionResponse, SearchFilters, SearchResult, StateEntry, EcphoriaClientOptions } from "./types.js";
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
export declare class EcphoriaClient {
    private readonly baseUrl;
    private readonly headers;
    private readonly timeout;
    constructor(options?: EcphoriaClientOptions);
    private request;
    private get;
    private post;
    private put;
    private del;
    /** Check server health. */
    health(): Promise<HealthResponse>;
    /** Execute a SQL query against the episodic store. Returns row dicts. */
    query(sql: string): Promise<Record<string, unknown>[]>;
    /** Ingest events into episodic memory. Returns the count of events ingested. */
    ingest(source: string, events: Event[]): Promise<number>;
    /** Semantic search by pre-computed vector. */
    search(vector: number[], options?: {
        k?: number;
        filters?: SearchFilters;
    }): Promise<SearchResult[]>;
    /** Semantic search by natural language text (embed + search in one call). */
    find(text: string, options?: {
        k?: number;
        filters?: SearchFilters;
    }): Promise<SearchResult[]>;
    /** Get agent state. Returns null if not found. */
    stateGet(agentId: string, key: string): Promise<StateEntry | null>;
    /** Set agent state. Returns the new version number. */
    stateSet(agentId: string, key: string, value: unknown): Promise<number>;
    /** Delete agent state. */
    stateDelete(agentId: string, key: string): Promise<void>;
    /** List all event sources. */
    sources(): Promise<string[]>;
    /** List all agent IDs. */
    agents(): Promise<string[]>;
    /** Trigger a backup of all stores. */
    backup(): Promise<BackupResponse>;
    /** Enforce data retention policy. */
    enforceRetention(): Promise<RetentionResponse>;
    /** Add a memory through the cognition pipeline (dedup / contradiction / importance). */
    memoryAdd(content: string, opts?: MemoryScope & {
        subject?: string;
        importance?: number;
        metadata?: Record<string, unknown>;
    }): Promise<Record<string, unknown>>;
    /** Hybrid (BM25 + vector) search over a scope's memories. Returns ranked hits. */
    memorySearch(query: string, opts?: MemoryScope & {
        k?: number;
    }): Promise<Record<string, unknown>[]>;
    /** List active memories in a scope. */
    memoryList(opts?: MemoryScope & {
        limit?: number;
    }): Promise<Record<string, unknown>[]>;
    /** Get a memory by id. Returns null if not found (or not in your tenant). */
    memoryGet(id: string): Promise<Record<string, unknown> | null>;
    /** Bi-temporal history for a memory's subject (oldest first). */
    memoryHistory(id: string): Promise<Record<string, unknown>[]>;
    /** Delete a memory by id. Returns false if it didn't exist (or not in your tenant). */
    memoryDelete(id: string): Promise<boolean>;
    /** A memory's source events + supersession chain (the audit trail behind a fact). */
    memoryProvenance(id: string): Promise<Record<string, unknown>>;
    /** Feedback so ranking learns: 'helpful' reinforces, 'wrong'/'obsolete' retires. */
    memoryFeedback(id: string, verdict: string): Promise<Record<string, unknown>>;
    /** Subjects with more than one active memory (the review queue). */
    memoryContradictions(userId?: string): Promise<Record<string, unknown>[]>;
    /** Resolve a contradiction: keep `keepId`, supersede the rest for `subject`. */
    memoryResolveContradiction(subject: string, keepId: string, userId?: string): Promise<Record<string, unknown>>;
    /** Add a graph edge (src -[relation]-> dst). supersede closes the prior (src, relation). */
    memoryLink(src: string, relation: string, dst: string, supersede?: boolean): Promise<Record<string, unknown>>;
    /** Edges around an entity (depth>1 expands the subgraph). */
    graphNeighbors(entity: string, depth?: number, limit?: number): Promise<Record<string, unknown>[]>;
    /** All knowledge-graph edges (bulk view). */
    graphEdges(limit?: number): Promise<Record<string, unknown>[]>;
    /** Degree + PageRank per node, optionally as-of a time. */
    graphCentrality(asOf?: string, limit?: number): Promise<Record<string, unknown>[]>;
    /** Shortest directed path between two entities (null if unreachable). */
    graphPath(src: string, dst: string, asOf?: string): Promise<string[] | null>;
    /** Community detection (connected clusters), optionally as-of a time. */
    graphCommunities(asOf?: string): Promise<string[][]>;
    /** Built-in memory templates. */
    memoryTemplates(): Promise<Record<string, unknown>[]>;
    /** Create a memory from a template + field values. */
    memoryFromTemplate(template: string, fields: Record<string, unknown>, userId?: string): Promise<Record<string, unknown>>;
    /** Upload a blob (image/PDF/audio). Optional caption stores a searchable memory. */
    attachmentUpload(data: Uint8Array, opts?: {
        contentType?: string;
        filename?: string;
        memoryId?: string;
        caption?: string;
    }): Promise<Record<string, unknown>>;
    /** List attachments (optionally for one memory). */
    attachmentList(memoryId?: string): Promise<Record<string, unknown>[]>;
    /** Delete an attachment. Returns false if it didn't exist. */
    attachmentDelete(id: string): Promise<boolean>;
    /** Re-embed active memories with the current provider (after a model change). */
    memoryReembed(limit?: number): Promise<Record<string, unknown>>;
    /** Consolidate a session's events into memory. */
    sessionDistill(sessionId: string): Promise<Record<string, unknown>>;
    /** Start a conversation session. */
    sessionStart(sessionId: string, agentId: string, opts?: {
        parentSessionId?: string;
        metadata?: Record<string, unknown>;
    }): Promise<Record<string, unknown>>;
    /** End a session, optionally attaching a summary. */
    sessionEnd(sessionId: string, summary?: string): Promise<Record<string, unknown>>;
    /** Recall all events recorded in a session. */
    sessionRecall(sessionId: string): Promise<Record<string, unknown>[]>;
    /** Create a durable agent/workflow run. Returns the run. */
    runCreate(opts?: {
        agentId?: string;
        input?: Record<string, unknown>;
        parentRunId?: string;
    }): Promise<Record<string, unknown>>;
    /** Get a run by id. */
    runGet(id: string): Promise<Record<string, unknown> | null>;
    /** List runs (newest first), optionally filtered by status. */
    runList(opts?: {
        status?: string;
        limit?: number;
    }): Promise<Record<string, unknown>[]>;
    /** Full step trace of a run (LLM/tool/HITL steps). */
    runTrace(id: string): Promise<Record<string, unknown>[]>;
    /** Cancel a run. */
    runCancel(id: string): Promise<Record<string, unknown>>;
    /** Run an agent end-to-end (durable LLM↔tool loop). Returns the resulting run. */
    runAgent(agentId: string, question: string, opts?: {
        maxTurns?: number;
    }): Promise<Record<string, unknown>>;
    /** Approve or reject a run awaiting approval (HITL). */
    runApprove(id: string, approve?: boolean): Promise<Record<string, unknown>>;
    /** Resume an approved run (durable resume after HITL). */
    runResume(id: string): Promise<Record<string, unknown>>;
    /** Register an event trigger: matching events start a run of `agentId`. */
    triggerRegister(name: string, agentId: string, opts?: {
        source?: string;
        eventType?: string;
    }): Promise<Record<string, unknown>>;
    /** List registered event triggers. */
    triggerList(): Promise<Record<string, unknown>[]>;
    /** Register a downstream MCP tool server. */
    toolRegister(name: string, url: string): Promise<Record<string, unknown>>;
    /** List registered downstream MCP tool servers. */
    toolList(): Promise<Record<string, unknown>[]>;
    /** Invoke a tool on a registered downstream MCP server. */
    toolCall(server: string, tool: string, args?: Record<string, unknown>): Promise<unknown>;
    /** Get Raft cluster status. */
    clusterStatus(): Promise<ClusterStatus>;
}
//# sourceMappingURL=client.d.ts.map