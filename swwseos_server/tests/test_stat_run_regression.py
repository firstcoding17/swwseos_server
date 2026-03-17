import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
SCRIPT_PATH = SCRIPT_DIR / "stat_run.py"

HAS_SCIPY = importlib.util.find_spec("scipy") is not None
HAS_STATSMODELS = importlib.util.find_spec("statsmodels") is not None


def run_stat(payload):
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"stat_run.py failed: code={proc.returncode}, stderr={proc.stderr.strip()}")

    stdout = (proc.stdout or "").strip()
    if not stdout:
        raise AssertionError("stat_run.py returned empty stdout")

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        # Fallback if any extra line appears in stdout.
        lines = [ln for ln in stdout.splitlines() if ln.strip()]
        if not lines:
            raise
        return json.loads(lines[-1])


def table_by_name(result, name):
    for t in result.get("tables", []):
        if str(t.get("name", "")) == name:
            return t
    return None


class StatRunRegressionTests(unittest.TestCase):
    @unittest.skipUnless(HAS_SCIPY, "scipy is required")
    def test_ttest_effect_size_ci_fields(self):
        rows = [{"v": i + 1, "g": "A"} for i in range(30)] + [{"v": i + 8, "g": "B"} for i in range(30)]
        out = run_stat({
            "op": "ttest",
            "rows": rows,
            "args": {"value": "v", "group": "g"},
        })

        self.assertTrue(out.get("ok"))
        stats = out.get("summary", {}).get("stats", {})
        self.assertIn("cohen_d", stats)
        self.assertIn("cohen_d_ci_low", stats)
        self.assertIn("cohen_d_ci_high", stats)
        lo = stats.get("cohen_d_ci_low")
        hi = stats.get("cohen_d_ci_high")
        if lo is not None and hi is not None:
            self.assertLessEqual(lo, hi)

    @unittest.skipUnless(HAS_SCIPY, "scipy is required")
    def test_chisq_effect_size_ci_fields(self):
        rows = []
        for i in range(70):
            if i % 3 == 0:
                rows.append({"a": "A", "b": "X"})
            elif i % 3 == 1:
                rows.append({"a": "B", "b": "Y"})
            else:
                rows.append({"a": "C", "b": "X"})

        out = run_stat({
            "op": "chisq",
            "rows": rows,
            "args": {"a": "a", "b": "b"},
        })

        self.assertTrue(out.get("ok"))
        stats = out.get("summary", {}).get("stats", {})
        self.assertIn("cramers_v", stats)
        self.assertIn("cramers_v_ci_low", stats)
        self.assertIn("cramers_v_ci_high", stats)
        lo = stats.get("cramers_v_ci_low")
        hi = stats.get("cramers_v_ci_high")
        if lo is not None and hi is not None:
            self.assertLessEqual(lo, hi)

    @unittest.skipUnless(HAS_SCIPY, "scipy is required")
    def test_anova_effect_size_ci_fields(self):
        rows = (
            [{"v": 10 + i % 5, "g": "G1"} for i in range(24)]
            + [{"v": 20 + i % 5, "g": "G2"} for i in range(24)]
            + [{"v": 30 + i % 5, "g": "G3"} for i in range(24)]
        )
        out = run_stat({
            "op": "anova",
            "rows": rows,
            "args": {"value": "v", "group": "g"},
        })

        self.assertTrue(out.get("ok"))
        stats = out.get("summary", {}).get("stats", {})
        self.assertIn("eta_sq", stats)
        self.assertIn("eta_sq_ci_low", stats)
        self.assertIn("eta_sq_ci_high", stats)
        lo = stats.get("eta_sq_ci_low")
        hi = stats.get("eta_sq_ci_high")
        if lo is not None and hi is not None:
            self.assertLessEqual(lo, hi)

    @unittest.skipUnless(HAS_SCIPY and HAS_STATSMODELS, "scipy and statsmodels are required")
    def test_pairwise_adjusted_has_raw_and_adjusted_p_columns(self):
        rows = (
            [{"v": 10 + i % 3, "g": "A"} for i in range(12)]
            + [{"v": 15 + i % 3, "g": "B"} for i in range(12)]
            + [{"v": 20 + i % 3, "g": "C"} for i in range(12)]
        )
        out = run_stat({
            "op": "pairwise_adjusted",
            "rows": rows,
            "args": {"value": "v", "group": "g", "pAdjust": "fdr_bh"},
        })

        self.assertTrue(out.get("ok"))
        tbl = table_by_name(out, "pairwise_adjusted")
        self.assertIsNotNone(tbl)
        cols = tbl.get("columns", [])
        self.assertIn("p_raw", cols)
        self.assertIn("p_adj", cols)
        self.assertIn("reject", cols)

    @unittest.skipUnless(HAS_STATSMODELS, "statsmodels is required")
    def test_ols_contains_bp_stats_and_diagnostic_figures(self):
        rows = []
        for i in range(1, 80):
            x1 = float(i)
            x2 = float(i % 7)
            noise = 0.3 * (i % 5)
            y = 2.0 * x1 + 1.2 * x2 + noise
            rows.append({"y": y, "x1": x1, "x2": x2})

        out = run_stat({
            "op": "ols",
            "rows": rows,
            "args": {"y": "y", "x": ["x1", "x2"]},
            "options": {"addIntercept": True, "dummy": True, "dropFirst": True},
        })

        self.assertTrue(out.get("ok"))
        stats = out.get("summary", {}).get("stats", {})
        for key in ("bp_lm", "bp_lm_p", "bp_f", "bp_f_p"):
            self.assertIn(key, stats)

        fig_types = {str(f.get("type", "")) for f in out.get("figures", [])}
        self.assertIn("residual_fitted", fig_types)
        self.assertIn("residual_qq", fig_types)

    def test_quality_process_includes_delta_stats_and_columns(self):
        rows = [
            {"x": 1, "y": 5, "g": "A"},
            {"x": 2, "y": 6, "g": "A"},
            {"x": 3, "y": 7, "g": "B"},
            {"x": 200, "y": 8, "g": "B"},
            {"x": None, "y": 9, "g": "B"},
            {"x": 4, "y": None, "g": "A"},
        ]
        out = run_stat({
            "op": "quality_process",
            "rows": rows,
            "args": {
                "strategy": "exclude",
                "method": "iqr",
                "columns": ["x"],
                "iqrK": 1.5,
                "zThresh": 3.0,
                "dropMissing": True,
            },
        })

        self.assertTrue(out.get("ok"))
        stats = out.get("summary", {}).get("stats", {})
        for key in (
            "rows_before",
            "rows_after",
            "rows_removed",
            "rows_with_missing_before",
            "rows_with_missing_after",
            "outlier_rows_before",
            "outlier_rows_after",
            "outlier_value_count_before",
            "outlier_value_count_after",
        ):
            self.assertIn(key, stats)

        tbl = table_by_name(out, "quality_process_summary")
        self.assertIsNotNone(tbl)
        cols = tbl.get("columns", [])
        self.assertIn("outlier_count_before", cols)
        self.assertIn("outlier_count_after", cols)
        self.assertIn("outlier_count_delta", cols)


if __name__ == "__main__":
    unittest.main()
