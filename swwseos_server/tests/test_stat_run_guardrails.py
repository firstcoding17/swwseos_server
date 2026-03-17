import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
SCRIPT_PATH = SCRIPT_DIR / "stat_run.py"


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
        raise AssertionError("No stdout lines returned from stat_run.py")
    return json.loads(lines[-1])


class StatRunGuardrailTests(unittest.TestCase):
    def test_invalid_json_returns_error_contract(self):
        proc = run_raw_stdin("{ invalid json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "INVALID_JSON")
        self.assertIn("message", out)
        self.assertIn("meta", out)

    def test_non_object_payload_returns_error_contract(self):
        proc = run_raw_stdin('["not","object"]')
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "INVALID_PAYLOAD")
        self.assertIn("details", out)

    def test_rows_must_be_array(self):
        proc = run_raw_stdin(json.dumps({
            "op": "describe",
            "rows": {"a": 1},
        }))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "INVALID_ROWS_TYPE")

    def test_unsupported_op_returns_json_error(self):
        proc = run_raw_stdin(json.dumps({
            "op": "not_supported",
            "rows": [{"a": 1}],
        }))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "UNSUPPORTED_OP")
        self.assertEqual(out.get("op"), "not_supported")

    def test_non_dict_args_does_not_crash(self):
        proc = run_raw_stdin(json.dumps({
            "op": "ttest",
            "rows": [{"v": 1, "g": "A"}, {"v": 2, "g": "B"}],
            "args": ["bad", "shape"],
        }))
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = parse_last_json(proc.stdout)
        self.assertIn("ok", out)
        self.assertEqual(out.get("op"), "ttest")
        self.assertIn("meta", out)


if __name__ == "__main__":
    unittest.main()
