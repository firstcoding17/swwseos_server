const { PythonShell } = require('python-shell');
PYTHON_BIN=/usr/bin/python3


exports.runPythonScript = (req, res) => {
  console.log("🚀 Received API request:", req.body);

  const { scriptName, args } = req.body;
  const options = {
    pythonPath: PYTHON_BIN, // ✅ 명확하게 지정
    args: args || [],
  };

  PythonShell.run(`./scripts/${scriptName}.py`, options, (err, results) => {
    if (err) {
      console.error('❌ Python script execution failed:', err);
      return res.status(500).send({ error: 'Python script execution failed', details: err.message });
    }

    try {
      console.log("✅ Python script output:", results);
      res.send({ output: results });
    } catch (parseError) {
      console.error('❌ Error parsing Python script output:', parseError);
      res.status(500).send({ error: 'Invalid output from Python script', details: parseError.message });
    }
  });
};
