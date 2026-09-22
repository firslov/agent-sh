/** Subprocess driver for mid-turn steering. */
import * as http from "node:http";
import { createCore } from "../../src/core/index.js";
import agentBackend from "../../src/agent/index.js";
import type { AppConfig, ExtensionContext } from "../../src/shell/host-types.js";
import type { AgentSurface } from "../../src/agent/host-types.js";

type Msg = { role: string; content?: unknown; tool_calls?: unknown[] };

function sse(body: Record<string, unknown>): string {
  return `data: ${JSON.stringify({ id: "x", object: "chat.completion.chunk", created: 0, model: "stub", ...body })}\n\n`;
}

function startStubLlm(onRequest: (n: number) => void): Promise<{
  baseURL: string; requests: Msg[][]; close: () => void;
}> {
  const requests: Msg[][] = [];
  let turn = 0;
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (req.url?.includes("/models")) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ data: [{ id: "stub" }] }));
        return;
      }
      try { requests.push((JSON.parse(body).messages ?? []) as Msg[]); } catch { requests.push([]); }
      const n = ++turn;
      res.writeHead(200, { "content-type": "text/event-stream" });

      onRequest(n);

      if (n === 1) {
        res.write(sse({ choices: [{ index: 0, delta: { role: "assistant", tool_calls: [
          { index: 0, id: "c1", type: "function", function: { name: "ls", arguments: JSON.stringify({ path: "." }) } },
          { index: 1, id: "c2", type: "function", function: { name: "ls", arguments: JSON.stringify({ path: "." }) } },
        ] } }] }));
        res.write(sse({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] }));
        res.write("data: [DONE]\n\n");
        res.end();
        return;
      }
      setTimeout(() => {
        res.write(sse({ choices: [{ index: 0, delta: { role: "assistant", content: `answer ${n}` }, finish_reason: "stop" }] }));
        res.write("data: [DONE]\n\n");
        res.end();
      }, 100);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      resolve({ baseURL: `http://127.0.0.1:${port}/v1`, requests, close: () => server.close() });
    });
  });
}

async function main() {
  const consumed: string[] = [];
  const queries: string[] = [];
  let bus: ReturnType<typeof createCore>["bus"];

  const llm = await startStubLlm((n) => {
    if (n === 1) bus.emit("agent:steer", { text: "STEER-A" });
    if (n === 2) bus.emit("agent:steer", { text: "STEER-B" });
  });

  const core = createCore({} as AppConfig);
  bus = core.bus;
  const ctx = core.extensionContext({ quit: () => {} });
  agentBackend(ctx);
  const agent = (ctx as ExtensionContext & { agent: AgentSurface }).agent;

  agent.providers.register({ id: "stub", apiKey: "stub", baseURL: llm.baseURL, models: [{ id: "stub" }] });
  core.bus.emit("core:extensions-loaded", { names: [] });
  await core.activateBackend("ash");
  await new Promise((r) => setImmediate(r));

  core.bus.on("agent:steer-consumed", ({ text }) => { consumed.push(text); });
  core.bus.on("agent:query", ({ query }) => { queries.push(query); });

  const done = new Promise<void>((r) => core.bus.on("agent:processing-done", () => r()));
  core.bus.emit("agent:submit", { query: "start" });
  await Promise.race([done, new Promise((r) => setTimeout(r, 15000))]);

  const roles = llm.requests.map((msgs) => msgs.map((m) => `${m.role}${m.tool_calls ? ":calls" : ""}`).join(","));
  const textOf = (m: Msg) => (typeof m.content === "string" ? m.content : "");
  const req2 = llm.requests[1] ?? [];
  const req3 = llm.requests[2] ?? [];
  const idxA = req2.findIndex((m) => m.role === "user" && textOf(m).includes("STEER-A"));
  const lastToolIdx = req2.map((m) => m.role).lastIndexOf("tool");

  process.stdout.write(JSON.stringify({
    requestCount: llm.requests.length,
    roles,
    steerAInSecondRequest: idxA !== -1,
    steerAAfterToolResults: idxA !== -1 && idxA > lastToolIdx,
    toolResultsInSecondRequest: req2.filter((m) => m.role === "tool").length,
    steerBInThirdRequest: req3.some((m) => m.role === "user" && textOf(m).includes("STEER-B")),
    consumed,
    queries,
  }) + "\n");
  llm.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("driver error:", err);
  process.exit(1);
});
