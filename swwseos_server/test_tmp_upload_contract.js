const assert = require("assert");
const http = require("http");
const express = require("express");
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
  const app = express();
  app.use(express.json({ limit: "2mb" }));
  app.post("/stat/run", (req, res) => {
    return res.json({
      ok: true,
      op: req.body?.op || "describe",
      summary: {
        title: req.body?.op === "recommend" ? "Recommended statistics" : "Statistics result",
        conclusion: `${req.body?.op || "describe"} completed`,
      },
      recommendations: req.body?.op === "recommend"
        ? [{ op: "corr", label: "Run correlation", reason: "Numeric columns are available." }]
        : [],
      warnings: [],
    });
  });
  app.post("/viz/aggregate", (req, res) => {
    return res.json({
      ok: true,
      data: {
        result: {
          x: ["east", "west"],
          y: [1, 2],
        },
        meta: {
          op: "groupby",
          rowsUsed: Array.isArray(req.body?.rows) ? req.body.rows.length : 0,
        },
      },
    });
  });
  app.post("/ml/run", (req, res) => {
    return res.json({
      ok: true,
      data: {
        task: req.body?.task || "anomaly",
        model: req.body?.model || "isolation_forest",
        rowsUsed: Array.isArray(req.body?.rows) ? req.body.rows.length : 0,
        metricsContract: {
          version: "v1",
          task: req.body?.task || "anomaly",
          primary: { name: "outlier_ratio", value: 0.12, goal: "lower" },
          items: [{ name: "outlier_ratio", value: 0.12, goal: "lower" }],
        },
        warnings: [],
      },
    });
  });
  app.use("/mcp", mcpRoutes);

  const previousInternalBase = process.env.MCP_INTERNAL_BASE;
  const server = await new Promise((resolve) => {
    const instance = app.listen(0, "127.0.0.1", () => resolve(instance));
  });
  process.env.MCP_INTERNAL_BASE = `http://127.0.0.1:${server.address().port}`;

  try {
    const toolsRes = await requestJson(server, "GET", "/mcp/tools");
    assert.equal(toolsRes.status, 200);
    assert.equal(toolsRes.body.ok, true);
    assert.ok(Array.isArray(toolsRes.body.data.tools));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "stat.run" && typeof tool.description === "string"));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "stat.recommend" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "viz.aggregate" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "ml.run" && tool.safe === false));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.list_datasets" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.compare_describe" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.compare_chart_plan" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.compare_stat_diff" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.recommend_analysis" && tool.safe === true));
    assert.ok(toolsRes.body.data.tools.some((tool) => tool.name === "workspace.formal_compare_plan" && tool.safe === true));
    pass("mcp tools expose enriched metadata");

    const chatRes = await requestJson(server, "POST", "/mcp/chat", {
      message: "what should I analyze first?",
      datasetContext: {
        datasetId: "ds-1",
        datasetName: "sales",
        rowCount: 120,
        columnCount: 4,
        columns: ["sales", "margin", "region", "date"],
        sampleRows: [
          { sales: 10, margin: 2, region: "east", date: "2026-01-01" },
          { sales: 11, margin: 2.5, region: "west", date: "2026-01-02" },
          { sales: "", margin: 2.2, region: "west", date: "2026-01-03" },
        ],
        profileSummary: {
          duplicates: 1,
          warnings: ["Sample size is small (<10 rows)."],
          topCorrCount: 2,
          topAnovaCount: 1,
        },
      },
    });
    assert.equal(chatRes.status, 200);
    assert.equal(chatRes.body.ok, true);
    assert.equal(chatRes.body.data.mode, "rule-based");
    assert.equal(typeof chatRes.body.data.reply, "string");
    assert.ok(Array.isArray(chatRes.body.data.suggestions));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.tool === "dataset.profile"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.tool === "stat.run"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.tool === "stat.recommend"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.tool === "viz.aggregate"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.tool === "ml.run"));
    assert.ok(Array.isArray(chatRes.body.data.toolCalls));
    assert.ok(chatRes.body.data.toolCalls.some((item) => item.tool === "stat.recommend"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.label === "Run anomaly detection starter"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.label === "Run regression starter"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.label === "Run classification starter"));
    assert.ok(chatRes.body.data.suggestions.some((item) => item.label === "Run time-series starter"));
    assert.ok(Array.isArray(chatRes.body.data.cards));
    assert.ok(chatRes.body.data.cards.some((card) => /recommend/i.test(String(card.title || ""))));
    assert.ok(chatRes.body.data.cards.some((card) => card.title === "Top recommended tests"));
    pass("mcp chat returns rule-based reply with suggestions");

    const statRecommendRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "stat.recommend",
      input: {
        rows: [
          { sales: 10, margin: 2, region: "east" },
          { sales: 11, margin: 2.5, region: "west" },
          { sales: 12, margin: 2.2, region: "west" },
        ],
        columns: ["sales", "margin", "region"],
      },
    });
    assert.equal(statRecommendRes.status, 200);
    assert.equal(statRecommendRes.body.ok, true);
    assert.equal(statRecommendRes.body.data.tool, "stat.recommend");
    assert.equal(statRecommendRes.body.data.result.ok, true);
    assert.equal(statRecommendRes.body.data.result.op, "recommend");
    pass("mcp stat.recommend tool proxies to stats recommender");

    const vizAggregateRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "viz.aggregate",
      input: {
        rows: [
          { region: "east", sales: 10 },
          { region: "west", sales: 12 },
          { region: "west", sales: 11 },
        ],
        columns: ["region", "sales"],
        spec: {
          type: "bar",
          x: "region",
          y: "sales",
          options: { agg: "sum" },
        },
      },
    });
    assert.equal(vizAggregateRes.status, 200);
    assert.equal(vizAggregateRes.body.ok, true);
    assert.equal(vizAggregateRes.body.data.tool, "viz.aggregate");
    assert.equal(vizAggregateRes.body.data.result.ok, true);
    assert.equal(vizAggregateRes.body.data.result.data.meta.op, "groupby");
    pass("mcp viz.aggregate tool proxies to visualization aggregation");

    const mlRunRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "ml.run",
      input: {
        task: "anomaly",
        model: "isolation_forest",
        rows: [
          { sales: 10, margin: 2, region: "east" },
          { sales: 12, margin: 3, region: "west" },
          { sales: 50, margin: 20, region: "west" },
        ],
        columns: ["sales", "margin", "region"],
      },
    });
    assert.equal(mlRunRes.status, 200);
    assert.equal(mlRunRes.body.ok, true);
    assert.equal(mlRunRes.body.data.tool, "ml.run");
    assert.equal(mlRunRes.body.data.result.ok, true);
    assert.equal(mlRunRes.body.data.result.data.task, "anomaly");
    pass("mcp ml.run tool proxies to ML runner");

    const mlRunHydratedRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "ml.run",
      input: {
        task: "anomaly",
        model: "isolation_forest",
      },
      datasetContext: {
        datasetId: "ds-ml",
        datasetName: "sales",
        columns: ["sales", "margin", "region"],
        sampleRows: [
          { sales: 10, margin: 2, region: "east" },
          { sales: 12, margin: 3, region: "west" },
          { sales: 50, margin: 20, region: "west" },
        ],
      },
    });
    assert.equal(mlRunHydratedRes.status, 200);
    assert.equal(mlRunHydratedRes.body.ok, true);
    assert.equal(mlRunHydratedRes.body.data.tool, "ml.run");
    assert.equal(mlRunHydratedRes.body.data.result.ok, true);
    assert.equal(mlRunHydratedRes.body.data.result.data.rowsUsed, 3);
    pass("mcp call hydrates ml.run input from dataset context");

    const workspaceChatRes = await requestJson(server, "POST", "/mcp/chat", {
      message: "compare open datasets in this workspace",
      datasetContext: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        rowCount: 120,
        columnCount: 4,
        columns: ["sales", "margin", "region", "date"],
        sampleRows: [
          { sales: 10, margin: 2, region: "east", date: "2026-01-01" },
          { sales: 11, margin: 2.5, region: "west", date: "2026-01-02" },
        ],
        workspaceDatasets: [
          { datasetId: "ds-1", name: "sales_jan", rowCount: 120, columnCount: 4, columns: ["sales", "margin", "region", "date"], active: true },
          { datasetId: "ds-2", name: "sales_feb", rowCount: 98, columnCount: 4, columns: ["sales", "margin", "region", "date"], active: false },
        ],
        profileSummary: {
          duplicates: 0,
          warnings: [],
          topCorrCount: 2,
          topAnovaCount: 1,
        },
      },
    });
    assert.equal(workspaceChatRes.status, 200);
    assert.equal(workspaceChatRes.body.ok, true);
    assert.ok(Array.isArray(workspaceChatRes.body.data.suggestions));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.list_datasets"));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.compare_describe"));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.compare_chart_plan"));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.compare_stat_diff"));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.recommend_analysis"));
    assert.ok(workspaceChatRes.body.data.suggestions.some((item) => item.tool === "workspace.formal_compare_plan"));
    assert.ok(String(workspaceChatRes.body.data.reply).includes("Workspace datasets"));
    pass("mcp chat supports workspace comparison context");

    const profileRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "dataset.profile",
      input: {
        datasetName: "sales",
        rowCount: 120,
        columns: ["sales", "margin", "region", "date"],
        sampleRows: [
          { sales: 10, margin: 2, region: "east", date: "2026-01-01" },
          { sales: "", margin: 2.5, region: "west", date: "2026-01-02" },
        ],
        profileSummary: { duplicates: 1, warnings: ["sample warning"], topCorrCount: 2, topAnovaCount: 1 },
      },
    });
    assert.equal(profileRes.status, 200);
    assert.equal(profileRes.body.ok, true);
    assert.equal(profileRes.body.data.tool, "dataset.profile");
    assert.equal(profileRes.body.data.result.ok, true);
    assert.equal(profileRes.body.data.result.data.datasetName, "sales");
    pass("mcp local dataset.profile tool returns profile summary");

    const flagsRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "dataset.flags",
      input: {
        datasetName: "sales",
        columns: ["sales", "margin", "region"],
        sampleRows: [
          { sales: 10, margin: 2, region: "east" },
          { sales: "", margin: 2.5, region: "east" },
          { sales: 11, margin: 2.3, region: "east" },
          { sales: 12, margin: 2.6, region: "east" },
          { sales: 13, margin: 2.9, region: "east" },
          { sales: 14, margin: 3.1, region: "east" },
          { sales: 15, margin: 3.4, region: "east" },
          { sales: 200, margin: 20, region: "west" },
        ],
        profileSummary: { duplicates: 1, warnings: ["sample warning"], topCorrCount: 2, topAnovaCount: 1 },
      },
    });
    assert.equal(flagsRes.status, 200);
    assert.equal(flagsRes.body.ok, true);
    assert.equal(flagsRes.body.data.tool, "dataset.flags");
    assert.equal(flagsRes.body.data.result.ok, true);
    assert.ok(Array.isArray(flagsRes.body.data.result.data.flags));
    assert.ok(flagsRes.body.data.result.data.flags.some((flag) => flag.kind === "outlier"));
    assert.ok(flagsRes.body.data.result.data.flags.some((flag) => flag.kind === "imbalance"));
    pass("mcp local dataset.flags tool returns flags");

    const workspaceRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.list_datasets",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          { datasetId: "ds-1", name: "sales_jan", rowCount: 120, columnCount: 4, columns: ["sales", "margin", "region", "date"], active: true },
          { datasetId: "ds-2", name: "sales_feb", rowCount: 98, columnCount: 4, columns: ["sales", "margin", "region", "date"], active: false },
        ],
      },
    });
    assert.equal(workspaceRes.status, 200);
    assert.equal(workspaceRes.body.ok, true);
    assert.equal(workspaceRes.body.data.tool, "workspace.list_datasets");
    assert.equal(workspaceRes.body.data.result.ok, true);
    assert.ok(Array.isArray(workspaceRes.body.data.result.data.datasets));
    assert.ok(Array.isArray(workspaceRes.body.data.result.data.sharedColumns));
    pass("mcp local workspace.list_datasets tool returns workspace comparison summary");

    const describeCompareRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.compare_describe",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          {
            datasetId: "ds-1",
            name: "sales_jan",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 10, margin: 2, region: "east" },
              { sales: 12, margin: 3, region: "west" },
              { sales: 11, margin: 2.5, region: "west" },
            ],
            active: true,
          },
          {
            datasetId: "ds-2",
            name: "sales_feb",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 8, margin: 1.5, region: "east" },
              { sales: 9, margin: 2, region: "west" },
              { sales: 10, margin: 2.2, region: "west" },
            ],
            active: false,
          },
        ],
      },
    });
    assert.equal(describeCompareRes.status, 200);
    assert.equal(describeCompareRes.body.ok, true);
    assert.equal(describeCompareRes.body.data.tool, "workspace.compare_describe");
    assert.equal(describeCompareRes.body.data.result.ok, true);
    assert.ok(Array.isArray(describeCompareRes.body.data.result.data.datasets));
    assert.ok(typeof describeCompareRes.body.data.result.data.focusNumericColumn === "string");
    pass("mcp local workspace.compare_describe tool returns side-by-side describe summary");

    const chartPlanRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.compare_chart_plan",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          {
            datasetId: "ds-1",
            name: "sales_jan",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 10, margin: 2, region: "east" },
              { sales: 12, margin: 3, region: "west" },
              { sales: 11, margin: 2.5, region: "west" },
            ],
            active: true,
          },
          {
            datasetId: "ds-2",
            name: "sales_feb",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 8, margin: 1.5, region: "east" },
              { sales: 9, margin: 2, region: "west" },
              { sales: 10, margin: 2.2, region: "west" },
            ],
            active: false,
          },
        ],
      },
    });
    assert.equal(chartPlanRes.status, 200);
    assert.equal(chartPlanRes.body.ok, true);
    assert.equal(chartPlanRes.body.data.tool, "workspace.compare_chart_plan");
    assert.equal(chartPlanRes.body.data.result.ok, true);
    assert.ok(typeof chartPlanRes.body.data.result.data.chartType === "string");
    assert.ok(Array.isArray(chartPlanRes.body.data.result.data.datasets));
    pass("mcp local workspace.compare_chart_plan tool returns chart comparison plan");

    const statDiffRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.compare_stat_diff",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          {
            datasetId: "ds-1",
            name: "sales_jan",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 10, margin: 2, region: "east" },
              { sales: 12, margin: 3, region: "west" },
              { sales: 11, margin: 2.5, region: "west" },
            ],
            active: true,
          },
          {
            datasetId: "ds-2",
            name: "sales_feb",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 18, margin: 5, region: "east" },
              { sales: 19, margin: 5.2, region: "east" },
              { sales: 20, margin: 5.5, region: "east" },
            ],
            active: false,
          },
        ],
      },
    });
    assert.equal(statDiffRes.status, 200);
    assert.equal(statDiffRes.body.ok, true);
    assert.equal(statDiffRes.body.data.tool, "workspace.compare_stat_diff");
    assert.equal(statDiffRes.body.data.result.ok, true);
    assert.ok(Array.isArray(statDiffRes.body.data.result.data.numericDiffs));
    assert.ok(Array.isArray(statDiffRes.body.data.result.data.categoricalDiffs));
    assert.ok(
      statDiffRes.body.data.result.data.numericDiffs.some(
        (item) => item?.action?.tool === "stat.run" && typeof item?.action?.inputTemplate?.op === "string"
      )
      || statDiffRes.body.data.result.data.categoricalDiffs.some(
        (item) => item?.action?.tool === "stat.run" && typeof item?.action?.inputTemplate?.op === "string"
      )
    );
    pass("mcp local workspace.compare_stat_diff tool returns statistical difference summary");

    const recommendRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.recommend_analysis",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          {
            datasetId: "ds-1",
            name: "sales_jan",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 10, margin: 2, region: "east" },
              { sales: 12, margin: 3, region: "west" },
              { sales: "", margin: 2.5, region: "west" },
            ],
            active: true,
          },
          {
            datasetId: "ds-2",
            name: "sales_feb",
            rowCount: 3,
            columnCount: 3,
            columns: ["sales", "margin", "region"],
            sampleRows: [
              { sales: 18, margin: 5, region: "east" },
              { sales: 19, margin: 5.2, region: "east" },
              { sales: 20, margin: 5.5, region: "east" },
            ],
            active: false,
          },
        ],
      },
    });
    assert.equal(recommendRes.status, 200);
    assert.equal(recommendRes.body.ok, true);
    assert.equal(recommendRes.body.data.tool, "workspace.recommend_analysis");
    assert.equal(recommendRes.body.data.result.ok, true);
    assert.ok(Array.isArray(recommendRes.body.data.result.data.recommendedDatasets));
    assert.ok(recommendRes.body.data.result.data.workspaceAction);
    assert.ok(
      recommendRes.body.data.result.data.recommendedDatasets.some(
        (item) => item?.action?.tool === "stat.run" && typeof item?.action?.inputTemplate?.op === "string"
      )
    );
    pass("mcp local workspace.recommend_analysis tool returns prioritized recommendations");

    const formalPlanRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "workspace.formal_compare_plan",
      input: {
        datasetId: "ds-1",
        datasetName: "sales_jan",
        workspaceDatasets: [
          {
            datasetId: "ds-1",
            name: "sales_jan",
            rowCount: 3,
            columnCount: 4,
            columns: ["sales", "margin", "region", "date"],
            sampleRows: [
              { sales: 10, margin: 2, region: "east", date: "2026-01-01" },
              { sales: 12, margin: 3, region: "west", date: "2026-01-02" },
              { sales: 11, margin: 2.5, region: "west", date: "2026-01-03" },
            ],
            active: true,
          },
          {
            datasetId: "ds-2",
            name: "sales_feb",
            rowCount: 3,
            columnCount: 4,
            columns: ["sales", "margin", "region", "date"],
            sampleRows: [
              { sales: 18, margin: 5, region: "east", date: "2026-02-01" },
              { sales: 19, margin: 5.2, region: "east", date: "2026-02-02" },
              { sales: 20, margin: 5.5, region: "east", date: "2026-02-03" },
            ],
            active: false,
          },
        ],
      },
    });
    assert.equal(formalPlanRes.status, 200);
    assert.equal(formalPlanRes.body.ok, true);
    assert.equal(formalPlanRes.body.data.tool, "workspace.formal_compare_plan");
    assert.equal(formalPlanRes.body.data.result.ok, true);
    assert.ok(Array.isArray(formalPlanRes.body.data.result.data.plans));
    assert.ok(Array.isArray(formalPlanRes.body.data.result.data.sharedColumns));
    assert.ok(
      formalPlanRes.body.data.result.data.plans.some(
        (plan) => plan?.action?.tool === "stat.run" && typeof plan?.action?.inputTemplate?.op === "string"
      )
    );
    pass("mcp local workspace.formal_compare_plan tool returns aligned compare plans");

    const missingMessageRes = await requestJson(server, "POST", "/mcp/chat", {
      datasetContext: {},
    });
    assert.equal(missingMessageRes.status, 400);
    assert.equal(missingMessageRes.body.ok, false);
    assert.equal(missingMessageRes.body.code, "MCP_CHAT_MESSAGE_REQUIRED");
    pass("mcp chat validates required message");

    const tooLongMessageRes = await requestJson(server, "POST", "/mcp/chat", {
      message: "x".repeat(2100),
      datasetContext: {},
    });
    assert.equal(tooLongMessageRes.status, 413);
    assert.equal(tooLongMessageRes.body.ok, false);
    assert.equal(tooLongMessageRes.body.code, "MCP_CHAT_MESSAGE_TOO_LARGE");
    pass("mcp chat enforces message size limit");

    const tooManyRowsRes = await requestJson(server, "POST", "/mcp/call", {
      tool: "stat.run",
      input: {
        op: "describe",
        rows: Array.from({ length: 501 }, (_, idx) => ({ value: idx })),
      },
    });
    assert.equal(tooManyRowsRes.status, 413);
    assert.equal(tooManyRowsRes.body.ok, false);
    assert.equal(tooManyRowsRes.body.code, "MCP_TOOL_ROWS_LIMIT");
    pass("mcp call enforces tool row limit");

    if (!process.exitCode) {
      console.log("[OK] MCP contract checks passed.");
    }
  } catch (e) {
    fail("mcp contract checks failed", e);
  } finally {
    if (previousInternalBase === undefined) delete process.env.MCP_INTERNAL_BASE;
    else process.env.MCP_INTERNAL_BASE = previousInternalBase;
    await new Promise((resolve) => server.close(resolve));
  }
}

run().catch((e) => {
  fail("unexpected test runner exception", e);
});
