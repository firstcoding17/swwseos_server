const assert = require("assert");
const http = require("http");
const express = require("express");
const { getClaudeConfig, isClaudeConfigured, runClaudeChat } = require("../services/claudeClient");
const mcpRoutes = require("../routes/mcp");

function pass(msg) {
  console.log(`[PASS] ${msg}`);
}

function fail(msg, err) {
  console.error(`[FAIL] ${msg}`);
  if (err) console.error(err);
  process.exitCode = 1;
}

function requestJson(server, method, path, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? Buffer.from(JSON.stringify(body), "utf8") : Buffer.from("", "utf8");
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port: server.address().port,
        path,
        method,
        headers: {
          "Content-Type": "application/json",
          "Content-Length": payload.length,
        },
      },
      (res) => {
        let raw = "";
        res.on("data", (d) => (raw += d.toString("utf8")));
        res.on("end", () => {
          try {
            resolve({
              status: res.statusCode,
              body: JSON.parse(raw || "{}"),
            });
          } catch (e) {
            reject(new Error(`invalid json: ${raw}`));
          }
        });
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function run() {
  const prevKey = process.env.ANTHROPIC_API_KEY;
  const prevModel = process.env.CLAUDE_MODEL;
  const prevTokens = process.env.CLAUDE_MAX_TOKENS;
  const prevFetch = global.fetch;

  try {
    delete process.env.ANTHROPIC_API_KEY;
    assert.equal(isClaudeConfigured(), false);
    pass("isClaudeConfigured is false without API key");

    process.env.ANTHROPIC_API_KEY = "test-key";
    process.env.CLAUDE_MODEL = "claude-test-model";
    process.env.CLAUDE_MAX_TOKENS = "777";

    const cfg = getClaudeConfig();
    assert.equal(cfg.apiKey, "test-key");
    assert.equal(cfg.model, "claude-test-model");
    assert.equal(cfg.maxTokens, 777);
    assert.equal(isClaudeConfigured(), true);
    pass("Claude config is loaded from env");

    let callCount = 0;
    const fetchImpl = async (_url, options) => {
      callCount += 1;
      const body = JSON.parse(options.body);
      if (callCount === 1) {
        assert.equal(body.model, "claude-test-model");
        assert.ok(Array.isArray(body.tools));
        assert.ok(body.tools.some((tool) => tool.name === "dataset.profile"));
        return {
          ok: true,
          text: async () => JSON.stringify({
            id: "msg_1",
            content: [
              {
                type: "tool_use",
                id: "toolu_1",
                name: "dataset.profile",
                input: {},
              },
            ],
          }),
        };
      }
      return {
        ok: true,
        text: async () => JSON.stringify({
          id: "msg_2",
          content: [
            {
              type: "text",
              text: "Dataset profile reviewed. Start with descriptive stats.",
            },
          ],
        }),
      };
    };

    let toolRunCount = 0;
    const result = await runClaudeChat({
      message: "What should I analyze first?",
      datasetContext: {
        datasetName: "sales",
        rowCount: 120,
        columnCount: 4,
        columns: ["sales", "margin", "region", "date"],
        sampleRows: [{ sales: 10, margin: 2, region: "east", date: "2026-01-01" }],
      },
      history: [{ role: "assistant", text: "Previous summary." }],
      toolDefinitions: [
        {
          name: "dataset.profile",
          description: "Profile dataset",
          input_schema: { type: "object", properties: {}, additionalProperties: true },
        },
      ],
      toolRunner: async (toolName, input) => {
        toolRunCount += 1;
        assert.equal(toolName, "dataset.profile");
        assert.deepEqual(input, {});
        return { ok: true, data: { datasetName: "sales" } };
      },
      fetchImpl,
    });

    assert.equal(result.mode, "claude");
    assert.equal(result.reply, "Dataset profile reviewed. Start with descriptive stats.");
    assert.ok(Array.isArray(result.toolCalls));
    assert.equal(result.toolCalls.length, 1);
    assert.equal(toolRunCount, 1);
    pass("Claude client executes tool-use loop and returns final reply");

    let routeCallCount = 0;
    global.fetch = async (_url, options) => {
      routeCallCount += 1;
      const body = JSON.parse(options.body);
      if (routeCallCount === 1) {
        assert.equal(body.model, "claude-test-model");
        assert.ok(Array.isArray(body.tools));
        assert.ok(body.tools.some((tool) => tool.name === "dataset.profile"));
        return {
          ok: true,
          text: async () => JSON.stringify({
            id: "msg_route_1",
            content: [
              {
                type: "tool_use",
                id: "toolu_route_1",
                name: "dataset.profile",
                input: {},
              },
            ],
          }),
        };
      }
      const toolResultMessage = body.messages.find(
        (message) => message?.role === "user"
          && Array.isArray(message.content)
          && message.content.some((block) => block?.type === "tool_result")
      );
      assert.ok(toolResultMessage);
      const toolResultBlock = toolResultMessage.content.find((block) => block?.type === "tool_result");
      const toolPayload = JSON.parse(toolResultBlock.content || "{}");
      assert.equal(toolPayload.ok, true);
      assert.equal(toolPayload.data?.datasetName, "sales");
      return {
        ok: true,
        text: async () => JSON.stringify({
          id: "msg_route_2",
          content: [
            {
              type: "text",
              text: "Claude route answer after dataset profile tool use.",
            },
          ],
        }),
      };
    };

    const app = express();
    app.use(express.json({ limit: "2mb" }));
    app.use("/mcp", mcpRoutes);
    const server = await new Promise((resolve) => {
      const instance = app.listen(0, "127.0.0.1", () => resolve(instance));
    });

    try {
      const routeRes = await requestJson(server, "POST", "/mcp/chat", {
        message: "What should I analyze first?",
        datasetContext: {
          datasetId: "ds-route",
          datasetName: "sales",
          rowCount: 120,
          columnCount: 4,
          columns: ["sales", "margin", "region", "date"],
          sampleRows: [
            { sales: 10, margin: 2, region: "east", date: "2026-01-01" },
            { sales: 12, margin: 2.6, region: "west", date: "2026-01-02" },
          ],
          profileSummary: {
            duplicates: 0,
            warnings: [],
            topCorrCount: 1,
            topAnovaCount: 1,
          },
        },
      });
      assert.equal(routeRes.status, 200);
      assert.equal(routeRes.body.ok, true);
      assert.equal(routeRes.body.data.mode, "claude");
      assert.equal(routeRes.body.data.reply, "Claude route answer after dataset profile tool use.");
      assert.ok(Array.isArray(routeRes.body.data.toolCalls));
      assert.equal(routeRes.body.data.toolCalls.length, 1);
      assert.equal(routeRes.body.data.toolCalls[0].tool, "dataset.profile");
      assert.ok(Array.isArray(routeRes.body.data.cards));
      assert.ok(routeRes.body.data.cards.length >= 1);
      pass("Claude-enabled /mcp/chat executes tool-use loop through the MCP route");
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }

    if (!process.exitCode) {
      console.log("[OK] Claude client contract checks passed.");
    }
  } catch (e) {
    fail("Claude client contract checks failed", e);
  } finally {
    if (prevKey === undefined) delete process.env.ANTHROPIC_API_KEY;
    else process.env.ANTHROPIC_API_KEY = prevKey;
    if (prevModel === undefined) delete process.env.CLAUDE_MODEL;
    else process.env.CLAUDE_MODEL = prevModel;
    if (prevTokens === undefined) delete process.env.CLAUDE_MAX_TOKENS;
    else process.env.CLAUDE_MAX_TOKENS = prevTokens;
    if (prevFetch === undefined) delete global.fetch;
    else global.fetch = prevFetch;
  }
}

run().catch((e) => {
  fail("unexpected test runner exception", e);
});
