import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
SCRIPT_PATH = SCRIPT_DIR / "ml_run.py"


def run_raw_stdin(stdin_text):
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        input=stdin_text,
        text=True,
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        check=False,
    )
    return proc


def parse_last_json(stdout_text):
    lines = [ln for ln in (stdout_text or "").splitlines() if ln.strip()]
    if not lines:
        raise AssertionError("No stdout lines returned from ml_run.py")
    return json.loads(lines[-1])


class MlRunGuardrailTests(unittest.TestCase):
    def test_invalid_json_returns_error_contract(self):
        proc = run_raw_stdin("{ invalid json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertIn("code", out)
        self.assertIn("message", out)

    def test_unsupported_op_returns_error_contract(self):
        proc = run_raw_stdin(json.dumps({"op": "not_supported"}))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "ML_OP_INVALID")

    def test_rows_required_guardrail(self):
        proc = run_raw_stdin(json.dumps({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": [],
            "args": {"target": "y", "features": ["x"]},
        }))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "ML_ROWS_REQUIRED")

    def test_target_required_guardrail(self):
        proc = run_raw_stdin(json.dumps({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": [{"x": 1}, {"x": 2}],
            "args": {"features": ["x"]},
        }))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "ML_TARGET_REQUIRED")

    def test_capabilities_contract_shape(self):
        proc = run_raw_stdin(json.dumps({"op": "capabilities"}))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertIn("ok", out)
        if out.get("ok") is True:
            data = out.get("data", {})
            self.assertIn("sklearn", data)
            self.assertIn("pandas", data)
            self.assertIn("deepLearningMode", data)
            self.assertIn("shap", data)
            self.assertIn("statsmodels", data)
            self.assertIn("pytorch_tabnet", data)
            self.assertIn("rtdl", data)
            self.assertIn("xgboost", data)
            self.assertIn("lightgbm", data)
            self.assertIn("catboost", data)
        else:
            self.assertIn("code", out)
            self.assertIn("message", out)


if __name__ == "__main__":
    unittest.main()
