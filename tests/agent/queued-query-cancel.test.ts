import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const DRIVER = fileURLToPath(new URL("../fixtures/queued-query-cancel-driver.ts", import.meta.url));

interface DriverResult { drained: boolean; cancelled: boolean }

function runDriver(): Promise<DriverResult> {
  const home = mkdtempSync(join(tmpdir(), "agent-sh-qc-"));
  return new Promise<DriverResult>((resolve, reject) => {
    const child = spawn("node", ["--import", "tsx", DRIVER], {
      env: {
        PATH: process.env.PATH,
        HOME: home,
        AGENT_SH_HOME: home,
        AGENT_SH_SKIP_SHELL_ENV: "1",
        OPENROUTER_API_KEY: "",
        OPENAI_API_KEY: "",
        DEEPSEEK_API_KEY: "",
        OPENAI_BASE_URL: "",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout!.on("data", (c) => { stdout += c.toString(); });
    child.stderr!.on("data", (c) => { stderr += c.toString(); });
    const timer = setTimeout(() => child.kill("SIGKILL"), 20000);
    child.on("close", (code) => {
      clearTimeout(timer);
      rmSync(home, { recursive: true, force: true });
      try {
        resolve(JSON.parse(stdout.trim().split(/\r?\n/).pop() ?? "") as DriverResult);
      } catch (err) {
        reject(new Error(`driver output not JSON.\nexit=${code}\nstdout:\n${stdout}\nstderr:\n${stderr}\n${(err as Error).message}`));
      }
    });
  });
}

// Regression: the finished query's finally used to null abortController
// unconditionally, wiping the controller a synchronously-queued follow-up had
// just installed — leaving that turn permanently uncancellable.
test("a query submitted synchronously from processing-done stays cancellable", async () => {
  const result = await runDriver();
  assert.equal(result.drained, true, "queued follow-up should have been submitted");
  assert.equal(result.cancelled, true, "agent:cancel-request should abort the follow-up turn");
});
