import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
SCRIPT_PATH = SCRIPT_DIR / "ml_run.py"

HAS_SKLEARN = importlib.util.find_spec("sklearn") is not None
HAS_PANDAS = importlib.util.find_spec("pandas") is not None


def run_ml(payload):
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"ml_run.py failed: code={proc.returncode}, stderr={proc.stderr.strip()}")
    stdout = (proc.stdout or "").strip()
    if not stdout:
        raise AssertionError("ml_run.py returned empty stdout")
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    return json.loads(lines[-1])


@unittest.skipUnless(HAS_SKLEARN and HAS_PANDAS, "sklearn and pandas are required")
class MlRunPhase1Tests(unittest.TestCase):
    def test_regression_elasticnet(self):
        rows = []
        for i in range(1, 80):
            x1 = float(i)
            x2 = float((i % 7) * 2)
            y = 3.5 * x1 + 1.2 * x2 + (i % 5) * 0.1
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "elasticnet",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"alpha": 0.01, "l1Ratio": 0.3, "testSize": 0.2},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "elasticnet")
        self.assertIn("r2", out.get("data", {}).get("metrics", {}))
        self.assertEqual(out.get("data", {}).get("errorAnalysis", {}).get("type"), "regression")

    def test_classification_knn(self):
        rows = []
        for i in range(100):
            x1 = float(i)
            x2 = float((i * 3) % 11)
            label = "A" if (x1 + x2) < 70 else "B"
            rows.append({"x1": x1, "x2": x2, "label": label})
        out = run_ml({
            "op": "train",
            "task": "classification",
            "model": "knn",
            "rows": rows,
            "args": {"target": "label", "features": ["x1", "x2"]},
            "options": {"nNeighbors": 5},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "knn")
        self.assertIn("accuracy", out.get("data", {}).get("metrics", {}))
        self.assertEqual(out.get("data", {}).get("errorAnalysis", {}).get("type"), "classification")
        self.assertEqual(out.get("data", {}).get("metricsContract", {}).get("task"), "classification")

    def test_anomaly_isolation_forest(self):
        rows = [{"x1": float(i), "x2": float(i + 1)} for i in range(30)]
        rows.extend([{"x1": 1000.0, "x2": 1200.0}, {"x1": -900.0, "x2": -1100.0}])
        out = run_ml({
            "op": "train",
            "task": "anomaly",
            "model": "isolation_forest",
            "rows": rows,
            "args": {"features": ["x1", "x2"]},
            "options": {"contamination": 0.1},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("task"), "anomaly")
        self.assertIn("outlier_count", out.get("data", {}).get("metrics", {}))
        self.assertEqual(out.get("data", {}).get("metricsContract", {}).get("task"), "anomaly")

    def test_clustering_kmeans(self):
        rows = []
        for i in range(25):
            rows.append({"x1": float(i), "x2": float(i + 1)})
        for i in range(25):
            rows.append({"x1": float(100 + i), "x2": float(101 + i)})
        out = run_ml({
            "op": "train",
            "task": "clustering",
            "model": "kmeans",
            "rows": rows,
            "args": {"features": ["x1", "x2"]},
            "options": {"nClusters": 2},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "kmeans")
        self.assertIn("cluster_count", out.get("data", {}).get("metrics", {}))
        self.assertTrue(out.get("data", {}).get("clusterSummary"))
        self.assertEqual(out.get("data", {}).get("metricsContract", {}).get("task"), "clustering")

    def test_dim_reduction_pca(self):
        rows = []
        for i in range(1, 60):
            rows.append({"x1": float(i), "x2": float(i * 2), "x3": float((i % 9) * 3)})
        out = run_ml({
            "op": "train",
            "task": "dim_reduction",
            "model": "pca",
            "rows": rows,
            "args": {"features": ["x1", "x2", "x3"]},
            "options": {"nComponents": 2},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "pca")
        self.assertEqual(len(out.get("data", {}).get("explainedVarianceRatio", [])), 2)
        self.assertTrue(out.get("data", {}).get("projectionPreview"))
        self.assertEqual(out.get("data", {}).get("metricsContract", {}).get("task"), "dim_reduction")

    def test_regression_extra_trees(self):
        rows = []
        for i in range(1, 90):
            x1 = float(i)
            x2 = float((i % 10) * 1.7)
            y = 1.7 * x1 + 0.9 * x2 + (i % 4) * 0.1
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "extra_trees",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"testSize": 0.2},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "extra_trees")
        self.assertIn("r2", out.get("data", {}).get("metrics", {}))

    def test_regression_preset_echo(self):
        rows = []
        for i in range(1, 120):
            x1 = float(i)
            x2 = float((i % 9) * 1.2)
            y = 2.1 * x1 + 0.8 * x2 + (i % 5) * 0.1
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "forest",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"preset": "fast"},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("preset"), "fast")

    def test_regression_permutation_importance(self):
        rows = []
        for i in range(1, 180):
            x1 = float(i)
            x2 = float((i % 13) * 1.1)
            y = 2.4 * x1 + 0.9 * x2 + (i % 6) * 0.15
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "forest",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"testSize": 0.2, "scoring": "r2"},
        })
        self.assertTrue(out.get("ok"))
        perm = out.get("data", {}).get("permutationImportance", [])
        self.assertIsInstance(perm, list)
        self.assertTrue(len(perm) > 0)
        self.assertIn("feature", perm[0])

    def test_regression_artifact_export(self):
        rows = []
        for i in range(1, 130):
            x1 = float(i)
            x2 = float((i % 10) * 1.4)
            y = 2.0 * x1 + 0.6 * x2 + (i % 5) * 0.1
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"includeArtifact": True},
        })
        self.assertTrue(out.get("ok"))
        artifact = out.get("data", {}).get("modelArtifact")
        self.assertIsInstance(artifact, dict)
        self.assertEqual(artifact.get("format"), "pickle-base64")
        self.assertTrue(artifact.get("byteSize", 0) > 0)

    def test_classification_adaboost(self):
        rows = []
        for i in range(120):
            x1 = float(i)
            x2 = float((i * 2) % 13)
            label = "A" if (x1 + x2) < 75 else "B"
            rows.append({"x1": x1, "x2": x2, "label": label})
        out = run_ml({
            "op": "train",
            "task": "classification",
            "model": "adaboost",
            "rows": rows,
            "args": {"target": "label", "features": ["x1", "x2"]},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "adaboost")
        self.assertIn("accuracy", out.get("data", {}).get("metrics", {}))

    def test_classification_svm(self):
        rows = []
        for i in range(120):
            x1 = float(i)
            x2 = float((i * 5) % 17)
            label = "A" if (x1 + x2) < 85 else "B"
            rows.append({"x1": x1, "x2": x2, "label": label})
        out = run_ml({
            "op": "train",
            "task": "classification",
            "model": "svm",
            "rows": rows,
            "args": {"target": "label", "features": ["x1", "x2"]},
            "options": {"svmC": 1.0, "svmKernel": "rbf", "svmGamma": "scale"},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "svm")
        self.assertIn("accuracy", out.get("data", {}).get("metrics", {}))

    def test_classification_calibrated(self):
        rows = []
        for i in range(140):
            x1 = float(i)
            x2 = float((i * 7) % 19)
            label = "A" if (x1 + x2) < 100 else "B"
            rows.append({"x1": x1, "x2": x2, "label": label})
        out = run_ml({
            "op": "train",
            "task": "classification",
            "model": "calibrated",
            "rows": rows,
            "args": {"target": "label", "features": ["x1", "x2"]},
            "options": {"calibMethod": "sigmoid", "calibCv": 3},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "calibrated")
        self.assertIn("accuracy", out.get("data", {}).get("metrics", {}))

    def test_regression_voting(self):
        rows = []
        for i in range(1, 120):
            x1 = float(i)
            x2 = float((i % 12) * 1.5)
            y = 2.8 * x1 + 0.5 * x2 + (i % 6) * 0.2
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "voting",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"testSize": 0.2},
        })
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("data", {}).get("model"), "voting")
        self.assertIn("r2", out.get("data", {}).get("metrics", {}))

    def test_regression_cv_scoring_contract(self):
        rows = []
        for i in range(1, 140):
            x1 = float(i)
            x2 = float((i % 9) * 1.3)
            y = 2.2 * x1 + 0.7 * x2 + (i % 7) * 0.2
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"cv": 5, "scoring": "neg_mean_absolute_error"},
        })
        self.assertTrue(out.get("ok"))
        validation = out.get("data", {}).get("validation", {})
        self.assertEqual(validation.get("scoring"), "neg_mean_absolute_error")
        self.assertTrue(validation.get("cv", {}).get("enabled"))

    def test_classification_cv_scoring_contract(self):
        rows = []
        for i in range(160):
            x1 = float(i)
            x2 = float((i * 2) % 21)
            label = "A" if (x1 + x2) < 120 else "B"
            rows.append({"x1": x1, "x2": x2, "label": label})
        out = run_ml({
            "op": "train",
            "task": "classification",
            "model": "linear",
            "rows": rows,
            "args": {"target": "label", "features": ["x1", "x2"]},
            "options": {"cv": 4, "scoring": "f1_weighted"},
        })
        self.assertTrue(out.get("ok"))
        validation = out.get("data", {}).get("validation", {})
        self.assertEqual(validation.get("scoring"), "f1_weighted")
        self.assertTrue(validation.get("cv", {}).get("enabled"))

    def test_success_result_contract_keys(self):
        rows = []
        for i in range(1, 140):
            x1 = float(i)
            x2 = float((i % 8) * 1.9)
            y = 1.3 * x1 + 0.4 * x2 + (i % 3) * 0.2
            rows.append({"x1": x1, "x2": x2, "y": y})
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": rows,
            "args": {"target": "y", "features": ["x1", "x2"]},
            "options": {"cv": 3, "scoring": "r2"},
        })
        self.assertTrue(out.get("ok"))
        data = out.get("data", {})
        self.assertIn("metrics", data)
        self.assertIn("importance", data)
        self.assertIn("permutationImportance", data)
        self.assertIn("shapImportance", data)
        self.assertIn("errorAnalysis", data)
        self.assertIn("modelArtifact", data)
        self.assertIn("diagnostics", data)
        self.assertIn("warnings", data)
        self.assertIn("metricsContract", data)
        self.assertIsInstance(data.get("metrics"), dict)
        self.assertIsInstance(data.get("importance"), list)
        self.assertIsInstance(data.get("permutationImportance"), list)
        self.assertIsInstance(data.get("shapImportance"), list)
        self.assertIsInstance(data.get("errorAnalysis"), dict)
        self.assertIn(type(data.get("modelArtifact")), {dict, type(None)})
        self.assertIsInstance(data.get("diagnostics"), dict)
        self.assertIsInstance(data.get("warnings"), list)
        self.assertIsInstance(data.get("metricsContract"), dict)
        self.assertIn("primary", data.get("metricsContract", {}))
        self.assertIn("items", data.get("metricsContract", {}))

class MlRunContractTests(unittest.TestCase):
    def test_invalid_task_returns_contract(self):
        out = run_ml({
            "op": "train",
            "task": "invalid_task",
            "model": "linear",
            "rows": [{"x1": 1.0, "y": 2.0}],
            "args": {"target": "y", "features": ["x1"]},
        })
        self.assertFalse(out.get("ok", True))
        self.assertEqual(out.get("code"), "ML_TASK_INVALID")

    def test_invalid_scoring_contract(self):
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "linear",
            "rows": [{"x1": 1.0, "y": 2.0}, {"x1": 2.0, "y": 3.0}, {"x1": 3.0, "y": 4.0}],
            "args": {"target": "y", "features": ["x1"]},
            "options": {"scoring": "not_a_real_metric", "cv": 2},
        })
        self.assertFalse(out.get("ok", True))
        self.assertIn(out.get("code"), {"ML_SCORING_INVALID", "ML_SKLEARN_REQUIRED", "ML_TOO_FEW_ROWS"})

    def test_phase3_dependency_contract(self):
        out = run_ml({
            "op": "train",
            "task": "regression",
            "model": "xgboost",
            "rows": [{"x1": 1.0, "y": 2.0}, {"x1": 2.0, "y": 3.0}, {"x1": 3.0, "y": 4.0}, {"x1": 4.0, "y": 5.0},
                     {"x1": 5.0, "y": 6.0}, {"x1": 6.0, "y": 7.0}, {"x1": 7.0, "y": 8.0}, {"x1": 8.0, "y": 9.0}],
            "args": {"target": "y", "features": ["x1"]},
        })
        if out.get("ok", False):
            self.assertEqual(out.get("data", {}).get("model"), "xgboost")
        else:
            self.assertIn(out.get("code"), {"ML_DEP_MISSING", "ML_SKLEARN_REQUIRED", "ML_MODEL_INVALID"})

    def test_timeseries_contract(self):
        rows = []
        for i in range(1, 40):
            rows.append({"ds": f"2026-01-{i:02d}", "y": float(i) + (i % 3) * 0.2})
        out = run_ml({
            "op": "train",
            "task": "timeseries",
            "model": "moving_avg",
            "rows": rows,
            "args": {"target": "y", "timeColumn": "ds"},
            "options": {"horizon": 6, "maWindow": 4},
        })
        if out.get("ok", False):
            data = out.get("data", {})
            self.assertEqual(data.get("task"), "timeseries")
            self.assertEqual(data.get("model"), "moving_avg")
            self.assertIn("mae", data.get("metrics", {}))
            self.assertIn("timeSeriesPreview", data)
        else:
            self.assertIn(out.get("code"), {"ML_SKLEARN_REQUIRED", "ML_TARGET_REQUIRED", "ML_MODEL_INVALID"})

    def test_deep_backend_dependency_contract(self):
        models = ["tabnet", "ft_transformer", "torch_mlp", "tf_mlp"]
        for model_name in models:
            out = run_ml({
                "op": "train",
                "task": "regression",
                "model": model_name,
                "rows": [{"x1": float(i), "y": float(i) + 0.1} for i in range(1, 20)],
                "args": {"target": "y", "features": ["x1"]},
            })
            if out.get("ok", False):
                self.assertEqual(out.get("data", {}).get("model"), model_name)
            else:
                self.assertIn(out.get("code"), {"ML_DEP_MISSING", "ML_SKLEARN_REQUIRED", "ML_MODEL_INVALID"})


if __name__ == "__main__":
    unittest.main()
