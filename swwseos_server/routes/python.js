const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const { spawn } = require('child_process');

const router = express.Router();
const uploadsDir = path.join(__dirname, '..', 'uploads');
const scriptsDir = path.join(__dirname, '..', 'scripts');

fs.mkdirSync(uploadsDir, { recursive: true });

const storage = multer.diskStorage({
  destination(_req, _file, callback) {
    callback(null, uploadsDir);
  },
  filename(_req, file, callback) {
    callback(null, `${Date.now()}-${file.originalname}`);
  },
});

const upload = multer({ storage });

function pythonCommand() {
  if (process.env.PYTHON_BIN) return process.env.PYTHON_BIN;
  return process.platform === 'win32' ? 'python' : 'python3';
}

function parseLastJson(stdout) {
  const lines = String(stdout || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) {
    throw new Error('python script returned empty stdout');
  }
  return JSON.parse(lines[lines.length - 1]);
}

function runPythonScript(scriptFile, args = [], options = {}) {
  const capture = options.capture || 'text';
  return new Promise((resolve, reject) => {
    const child = spawn(pythonCommand(), [path.join(scriptsDir, scriptFile), ...args], {
      cwd: path.join(__dirname, '..'),
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUTF8: '1',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const stdoutChunks = [];
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdoutChunks.push(Buffer.from(chunk));
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        return reject(new Error(stderr.trim() || `python exited with code ${code}`));
      }
      const stdoutBuffer = Buffer.concat(stdoutChunks);
      if (capture === 'buffer') return resolve(stdoutBuffer);
      return resolve(stdoutBuffer.toString('utf8'));
    });
  });
}

router.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ ok: false, error: 'file upload failed' });
  }
  return res.json({
    ok: true,
    message: 'file uploaded',
    filename: req.file.filename,
  });
});

router.post('/process', async (req, res) => {
  try {
    const filename = String(req.body?.filename || '').trim();
    if (!filename) {
      return res.status(400).json({ ok: false, error: 'filename is required' });
    }

    const filePath = path.join(uploadsDir, filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ ok: false, error: 'uploaded file not found' });
    }

    const stdout = await runPythonScript('load_file.py', [filePath]);
    return res.json(parseLastJson(stdout));
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: 'python process failed',
      details: error.message,
    });
  }
});

router.post('/generate-graph', async (req, res) => {
  try {
    const { xColumn, yColumn, data } = req.body || {};
    if (!xColumn || !yColumn || !Array.isArray(data)) {
      return res.status(400).json({
        ok: false,
        error: 'xColumn, yColumn, and data are required',
      });
    }

    const imageBuffer = await runPythonScript(
      'generate_graph.py',
      [JSON.stringify(data), String(xColumn), String(yColumn)],
      { capture: 'buffer' }
    );

    return res.json({
      ok: true,
      image: imageBuffer.toString('base64'),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: 'graph generation failed',
      details: error.message,
    });
  }
});

router.post('/run-python', async (req, res) => {
  try {
    const scriptName = String(req.body?.scriptName || '').trim();
    const args = Array.isArray(req.body?.args) ? req.body.args.map((arg) => String(arg)) : [];
    if (!scriptName || scriptName.includes('/') || scriptName.includes('\\')) {
      return res.status(400).json({
        ok: false,
        error: 'scriptName is required',
      });
    }

    const stdout = await runPythonScript(`${scriptName}.py`, args);
    const lines = String(stdout || '')
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    return res.json({
      ok: true,
      output: lines,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: 'python script execution failed',
      details: error.message,
    });
  }
});

module.exports = router;
