/** Subprocess driver for the queued-follow-up cancel test.
 *  Submits query A, drains a queued query B synchronously from
 *  processing-done (what ashi does), then cancels B's long-running turn. */
import * as http from "node:http";
import { createCore } from "../../src/core/index.js";
import agentBackend from "../../src/agent/index.js";
import type { AppConfig, ExtensionContext } from "../../src/shell/host-types.js";
import type { AgentSurface } from "../../src/agent/host-types.js";

/** Turn A answers with text; turn B hangs so the cancel has something to abort. */
function startStubLlm(): Promise<{ baseURL: string; close: () => void }> {
  let turn = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      if (req.url?.includes("/models")) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ data: [{ id: "stub" }] }));
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream" });
      if (++turn > 1) return; // turn B: never respond
      res.write(`data: ${JSON.stringify({
        id: "a", object: "chat.completion.chunk", created: 0, model: "stub",
        choices: [{ index: 0, delta: { role: "assistant", content: "A done." }, finish_reason: "stop" }],
      })}\n\n`);
      res.write("data: [DONE]\n\n");
      res.end();
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      resolve({ baseURL: `http://127.0.0.1:${port}/v1`, close: () => server.close() });
    });
  });
}

async function main() {
  const llm = await startStubLlm();
  const core = createCore({} as AppConfig);
  const ctx = core.extensionContext({ quit: () => {} });
  agentBackend(ctx);
  const agent = (ctx as ExtensionContext & { agent: AgentSurface }).agent;

  agent.providers.register({
    id: "stub",
    apiKey: "stub",
    baseURL: llm.baseURL,
    models: [{ id: "stub" }],
  });
  core.bus.emit("core:extensions-loaded", { names: [] });
  await core.activateBackend("ash");
  await new Promise((r) => setImmediate(r));

  let drained = false;
  core.bus.on("agent:processing-done", () => {
    if (drained) return;
    drained = true;
    core.bus.emit("agent:submit", { query: "B" }); // synchronous drain, as ashi does
  });

  let cancelled = false;
  core.bus.on("agent:cancelled", () => { cancelled = true; });

  core.bus.emit("agent:submit", { query: "A" });

  await new Promise((r) => setTimeout(r, 1500)); // let A finish and B start
  core.bus.emit("agent:cancel-request", {});
  await new Promise((r) => setTimeout(r, 500));

  process.stdout.write(JSON.stringify({ drained, cancelled }) + "\n");
  llm.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("driver error:", err);
  process.exit(1);
});
