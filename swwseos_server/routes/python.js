const express = require('express');
const multer = require('multer');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const router = express.Router();
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';

const storage = multer.diskStorage({
  destination(req, file, cb) {
    cb(null, path.join(__dirname, '../uploads'));
  },
  filename(req, file, cb) {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

const uploadData = multer({ storage });

router.post('/run-python', (req, res) => {
  const scriptName = String(req.body?.scriptName || '').trim();
  const args = Array.isArray(req.body?.args) ? req.body.args.map((v) => String(v)) : [];

  if (!/^[A-Za-z0-9_-]+$/.test(scriptName)) {
    return res.status(400).json({
      ok: false,
      code: 'RUN_PYTHON_INVALID_SCRIPT',
      message: 'invalid script name',
      error: 'invalid script name',
    });
  }

  const scriptsDir = path.resolve(__dirname, '../scripts');
  const scriptPath = path.join(scriptsDir, `${scriptName}.py`);
  if (!fs.existsSync(scriptPath)) {
    return res.status(404).json({
      ok: false,
      code: 'RUN_PYTHON_SCRIPT_NOT_FOUND',
      message: 'python script not found',
      details: { scriptName },
      error: 'python script not found',
    });
  }

  const py = spawn(PYTHON_BIN, [scriptPath, ...args], {
    env: { ...process.env, PYTHONUTF8: '1' },
    cwd: path.resolve(__dirname, '..'),
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let out = '';
  let err = '';
  py.stdout.on('data', (d) => (out += d.toString()));
  py.stderr.on('data', (d) => (err += d.toString()));

  py.on('close', (code) => {
    if (code !== 0) {
      return res.status(500).json({
        ok: false,
        code: 'RUN_PYTHON_FAILED',
        message: 'python script execution failed',
        details: err,
        error: 'python script execution failed',
      });
    }
    const text = out.trim();
    try {
      return res.json({ ok: true, output: JSON.parse(text || '{}') });
    } catch {
      return res.json({ ok: true, output: text });
    }
  });

  return undefined;
});

router.post('/upload', uploadData.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      ok: false,
      code: 'UPLOAD_MISSING_FILE',
      message: 'file upload failed',
      error: 'file upload failed',
    });
  }

  return res.json({
    ok: true,
    message: 'file upload complete',
    filename: req.file.filename,
  });
});

router.post('/process', (req, res) => {
  const filename = req.body?.filename;
  if (!filename) {
    return res.status(400).json({
      ok: false,
      code: 'PROCESS_MISSING_FILENAME',
      message: 'file process failed: missing filename',
      error: 'file process failed: missing filename',
    });
  }

  const filePath = path.join(__dirname, '../uploads', filename);

  const pythonProcess = spawn(PYTHON_BIN, ['scripts/load_file.py', filePath], {
    env: { ...process.env, PYTHONUTF8: '1' },
  });

  let out = '';
  let err = '';

  pythonProcess.stdout.on('data', (data) => {
    out += data.toString();
  });

  pythonProcess.stderr.on('data', (data) => {
    err += data.toString();
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
      return res.status(500).json({
        ok: false,
        code: 'PROCESS_PYTHON_FAILED',
        message: 'python script failed',
        details: err,
        error: 'python script failed',
      });
    }

    try {
      const parsed = JSON.parse(out);
      if (parsed && typeof parsed === 'object' && Object.prototype.hasOwnProperty.call(parsed, 'ok')) {
        return res.json(parsed);
      }
      return res.json({ ok: true, ...(parsed || {}) });
    } catch (parseErr) {
      return res.status(500).json({
        ok: false,
        code: 'PROCESS_INVALID_JSON',
        message: 'invalid json from python',
        details: String(parseErr),
        error: 'invalid json from python',
      });
    }
  });

  return undefined;
});

router.post('/generate-graph', (req, res) => {
  const { xColumn, yColumn, data } = req.body || {};

  if (!xColumn || !yColumn || !data) {
    return res.status(400).json({
      ok: false,
      code: 'GRAPH_MISSING_ARGS',
      message: 'required graph args are missing',
      error: 'required graph args are missing',
    });
  }

  const pythonProcess = spawn(PYTHON_BIN, [
    'scripts/generate_graph.py',
    JSON.stringify(data),
    xColumn,
    yColumn,
  ]);

  const imageBuffer = [];
  let err = '';

  pythonProcess.stdout.on('data', (chunk) => {
    imageBuffer.push(chunk);
  });

  pythonProcess.stderr.on('data', (chunk) => {
    err += chunk.toString();
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
      return res.status(500).json({
        ok: false,
        code: 'GRAPH_PYTHON_FAILED',
        message: 'graph generation failed',
        details: err,
        error: 'graph generation failed',
      });
    }

    const image = Buffer.concat(imageBuffer).toString('base64');
    return res.json({ ok: true, image });
  });

  return undefined;
});

module.exports = router;
