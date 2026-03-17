const assert = require("assert");
const http = require("http");
const express = require("express");
const mcpRoutes = require("../routes/mcp");
const statRoutes = require("../routes/stat");
const vizRoutes = require("../routes/viz");
const mlRoutes = require("../routes/ml");

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

function buildDatasetContext() {
  return {
    datasetId: "ds-manual",
    datasetName: "retail_sales",
    rowCount: 120,
    columnCount: 4,
    columns: ["sales", "margin", "region", "date"],
    sampleRows: [
      { sales: 10, margin: 2.0, region: "east", date: "2026-01-01" },
      { sales: 12, margin: 2.6, region: "west", date: "2026-01-02" },
      { sales: 11, margin: 2.3, region: "west", date: "2026-01-03" },
      { sales: 15, margin: 3.9, region: "east", date: "2026-01-04" },
      { sales: 16, margin: 4.1, region: "south", date: "2026-01-05" },
      { sales: 18, margin: 4.8, region: "south", date: "2026-01-06" },
    ],
    profileSummary: {
      duplicates: 0,
      warnings: [],
      topCorrCount: 2,
      topAnovaCount: 1,
    },
  };
}

async function run() {
  const app = express();
  app.use(express.json({ limit: "5mb" }));
  app.use("/mcp", mcpRoutes);
  app.use("/stat", statRoutes);
  app.use("/viz", vizRoutes);
  app.use("/ml", mlRoutes);

  const previousBase = process.env.MCP_INTERNAL_BASE;
  const previousPort = process.env.PORT;

  const server = await new Promise((resolve) => {
    const instance = app.listen(0, "127.0.0.1", () => resolve(instance));
  });

  process.env.MCP_INTERNAL_BASE = `http://127.0.0.1:${server.address().port}`;
  process.env.PORT = String(server.address().port);

  try {
    const datasetContext = buildDatasetContext();

    const chatRes = await requestJson(server, "POST", "/mcp/chat", {
      message: "what should I analyze first?",
      datasetContext,
    });
    assert.equal(chatRes.status, 200);
    assert.equal(chatRes.body.ok, true);
    assert.equal(chatRes.body.data.mode, "rule-based");
    assert.ok(Array.isArray(chatRes.body.data.cards));
    assert.ok(chatRes.body.data.cards.length >= 1);
    assert.ok(Array.isArray(chatRes.body.data.suggestions));
    assert.ok(Array.isArray(chatRes.body.data.toolCalls));
    assert.ok(chatRes.body.data.toolCalls.some((item) => item.tool === "stat.recommend"));
    const statSuggestion = chatRes.body.data.suggestions.find(
      (item) => item?.tool === "stat.run" && item?.inputTemplate?.op === "describe"
    );
    assert.ok(statSuggestion);
    pass("mcp manual flow starts with chat insights, stats recommender context, and a runnable stats suggestion");

    const statRes = await requestJson(server, "POST", "/mcp/call", {
      tool: statSuggestion.tool,
      input: statSuggestion.inputTemplate,
      datasetContext,
    });
    assert.equal(statRes.status, 200);
    assert.equal(statRes.body.ok, true);
    assert.equal(statRes.body.data.tool, "stat.run");
    assert.equal(statRes.body.data.result.ok, true);
    assert.equal(statRes.body.data.result.op, "describe");
    assert.equal(typeof statRes.body.data.result.summary?.title, "string");
    pass("mcp manual flow can execute the suggested stats action end-to-end");

    const vizRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "viz.prepare",
      input: {
        spec: {
          type: "scatter",
          x: "sales",
          y: "margin",
          options: {
            title: "Sales vs Margin",
            xLabel: "Sales",
            yLabel: "Margin",
          },
        },
      },
      datasetContext,
    });
    assert.equal(vizRes.status, 200);
    assert.equal(vizRes.body.ok, true);
    assert.equal(vizRes.body.data.tool, "viz.prepare");
    assert.equal(vizRes.body.data.result.ok, true);
    assert.equal(typeof vizRes.body.data.result.data?.fig_json, "string");
    pass("mcp manual flow can prepare a visualization result through the MCP bridge");

    const mlRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "ml.capabilities",
      input: {},
      datasetContext,
    });
    assert.equal(mlRes.status, 200);
    assert.equal(mlRes.body.ok, true);
    assert.equal(mlRes.body.data.tool, "ml.capabilities");
    assert.equal(mlRes.body.data.result.ok, true);
    assert.equal(typeof mlRes.body.data.result.data?.sklearn, "boolean");
    pass("mcp manual flow can inspect ML capability results through the MCP bridge");

    if (!process.exitCode) {
      console.log("[OK] MCP manual-flow checks passed.");
    }
  } catch (e) {
    fail("mcp manual-flow checks failed", e);
  } finally {
    if (previousBase === undefined) delete process.env.MCP_INTERNAL_BASE;
    else process.env.MCP_INTERNAL_BASE = previousBase;

    if (previousPort === undefined) delete process.env.PORT;
    else process.env.PORT = previousPort;

    await new Promise((resolve) => server.close(resolve));
  }
}

run();
