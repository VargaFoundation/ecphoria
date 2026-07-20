// Node test for the admin console's pure UI render helpers (esc / memRowsHTML / renderTimeline).
//
// The console is a single self-contained HTML file baked into the binary via include_str!, so it
// can't be browser-tested in CI. This harness extracts its <script>, evaluates it against a minimal
// DOM stub (enough for the top-level init not to throw), then unit-tests the pure string functions —
// closing the "frontend logic is untested" gap without a full browser.
//
//   node scripts/test-admin-ui.mjs

import fs from "node:fs";
import vm from "node:vm";
import assert from "node:assert";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = fs.readFileSync(
  path.join(root, "crates/ecphoria-gateway/src/rest/admin_ui.html"),
  "utf8",
);
const script = html.split("<script>")[1].split("</script>")[0];

// Minimal DOM stub: every element is a permissive proxy; querySelectorAll returns [].
const makeEl = () =>
  new Proxy(
    { innerHTML: "", textContent: "", value: "", checked: false, dataset: {}, style: {} },
    {
      get(t, p) {
        if (p === "addEventListener" || p === "setAttribute") return () => {};
        if (p === "closest") return () => null;
        if (p === "classList") return { add() {}, remove() {} };
        return t[p];
      },
      set(t, p, v) {
        t[p] = v;
        return true;
      },
    },
  );

const document = {
  getElementById: () => makeEl(),
  querySelectorAll: () => [],
  addEventListener: () => {},
};
const sandbox = {
  document,
  window: {},
  location: { origin: "http://localhost:8432" },
  console,
  fetch: async () => ({ ok: true, text: async () => "{}", json: async () => ({}) }),
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

// Run the console script, then capture the (const/function-scoped) helpers we want to test.
vm.runInContext(
  script + "\nglobalThis.__t = { esc, memRowsHTML, renderTimeline };",
  sandbox,
);
const { esc, memRowsHTML, renderTimeline } = sandbox.__t;

// esc escapes HTML metacharacters.
assert.strictEqual(esc("<b>&\"'"), "&lt;b&gt;&amp;&quot;&#39;");

// memRowsHTML: escapes content, exposes actions with ids in data-* attrs (search vs browse shapes).
const rows = memRowsHTML([
  { memory: { id: "id1", content: "<script>x</script>", subject: "s" }, score: 0.5 },
]);
assert.ok(rows.includes("&lt;script&gt;x&lt;/script&gt;"), "content is escaped");
assert.ok(rows.includes('data-mem-del="id1"'), "delete action carries the id");
assert.ok(rows.includes('data-mem-hist="id1"'), "history action present");
assert.ok(rows.includes("0.500"), "score formatted");
// Browse shape ([memory] with no score) → score shown as em dash.
assert.ok(memRowsHTML([{ id: "id2", content: "c", subject: "" }]).includes("—"));

// renderTimeline: version rows with state + validity, active flagged; escapes content.
const tl = renderTimeline({
  history: [
    { state: "active", version: 3, valid_from: "2026-01-01", valid_to: null, content: "<x>" },
    { state: "superseded", version: 2, valid_from: "2025-01-01", valid_to: "2026-01-01", content: "old" },
  ],
});
assert.ok(tl.includes("tl-row active"), "active version flagged");
assert.ok(tl.includes("→ now"), "open validity renders as 'now'");
assert.ok(tl.includes("&lt;x&gt;"), "timeline escapes content");
assert.ok(renderTimeline({ history: [] }).includes("no history"));

console.log("admin-ui UI logic tests: all passed");
