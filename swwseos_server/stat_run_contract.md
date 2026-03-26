const assert = require("assert");
const http = require("http");
const express = require("express");
const tmpUploadRoutes = require("../routes/tmp-upload");

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
          let parsed = null;
          try {
            parsed = JSON.parse(raw || "{}");
          } catch (e) {
            return reject(new Error(`invalid JSON response: ${raw}`));
          }
          resolve({ status: res.statusCode, body: parsed });
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
  app.use(express.json({ limit: "1mb" }));
  app.use("/tmp-upload", tmpUploadRoutes);

  const server = await new Promise((resolve) => {
    const s = app.listen(0, "127.0.0.1", () => resolve(s));
  });

  try {
    const signName = "sample data.csv";
    const signRes = await requestJson(server, "POST", "/tmp-upload/sign", { name: signName });
    assert.equal(signRes.status, 200);
    assert.equal(signRes.body.ok, true);
    assert.ok(signRes.body.data);
    assert.equal(typeof signRes.body.data.url, "string");
    assert.ok(signRes.body.data.url.includes(encodeURIComponent(signName)));
    assert.equal(typeof signRes.body.data.key, "string");
    assert.ok(signRes.body.data.key.startsWith("tmp/"));
    assert.equal(typeof signRes.body.data.ttlSec, "number");
    assert.ok(signRes.body.data.ttlSec > 0);
    pass("tmp-upload sign returns { ok:true, data:{url,key,ttlSec} }");

    const delKey = "tmp/test-key-123";
    const delRes = await requestJson(server, "POST", "/tmp-upload/delete", { key: delKey });
    assert.equal(delRes.status, 200);
    assert.equal(delRes.body.ok, true);
    assert.ok(delRes.body.data);
    assert.equal(delRes.body.data.key, delKey);
    pass("tmp-upload delete echoes deleted key contract");

    if (!process.exitCode) {
      console.log("[OK] tmp-upload contract checks passed.");
    }
  } catch (e) {
    fail("tmp-upload contract checks failed", e);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

run().catch((e) => {
  fail("unexpected test runner exception", e);
});
