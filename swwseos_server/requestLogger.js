{
  "name": "swwseos_server",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node app.js",
    "test": "echo \"Error: no test specified\" && exit 1",
    "test:stat": "python -m unittest discover -s tests -p \"test_stat_run_*.py\" -v",
    "test:ml": "python -m unittest discover -s tests -p \"test_ml_run_*.py\" -v",
    "test:tmp-upload": "node tests/test_tmp_upload_contract.js",
    "test:mcp": "node tests/test_mcp_contract.js",
    "test:mcp-manual": "node tests/test_mcp_manual_flow.js",
    "test:claude": "node tests/test_claude_client_contract.js"
  },
  "keywords": [],
  "author": "",
  "license": "ISC",
  "description": "",
  "dependencies": {
    "body-parser": "^1.20.3",
    "cors": "^2.8.5",
    "express": "^4.21.2",
    "multer": "^1.4.5-lts.1",
    "python-shell": "^5.0.0",
    "socket.io": "^4.8.1"
  }
}
