const { spawn } = require('child_process');
const path = require('path');

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

function runPythonJson(scriptRelativePath, payload = {}) {
  const scriptPath = path.join(__dirname, '..', scriptRelativePath);
  return new Promise((resolve, reject) => {
    const child = spawn(pythonCommand(), [scriptPath], {
      cwd: path.dirname(scriptPath),
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUTF8: '1',
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        return reject(new Error(`python exited with code ${code}: ${stderr.trim()}`));
      }
      try {
        return resolve(parseLastJson(stdout));
      } catch (error) {
        return reject(new Error(`failed to parse python json: ${error.message}\nstdout=${stdout}\nstderr=${stderr}`));
      }
    });

    child.stdin.write(JSON.stringify(payload || {}));
    child.stdin.end();
  });
}

module.exports = {
  runPythonJson,
};
