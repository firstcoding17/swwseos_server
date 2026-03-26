import base64
import importlib.util
import json
import math
import pickle
import sys
import time

HAS_PANDAS = importlib.util.find_spec("pandas") is not None
HAS_NUMPY = importlib.util.find_spec("numpy") is not None
HAS_SKLEARN = importlib.util.find_spec("sklearn") is not None
HAS_SHAP = importlib.util.find_spec("shap") is not None
HAS_STATSMODELS = importlib.util.find_spec("statsmodels") is not None
HAS_XGBOOST = importlib.util.find_spec("xgboost") is not None
HAS_LIGHTGBM = importlib.util.find_spec("lightgbm") is not None
HAS_CATBOOST = importlib.util.find_spec("catboost") is not None
HAS_PYTORCH_TABNET = importlib.util.find_spec("pytorch_tabnet") is not None
HAS_RTDL = importlib.util.find_spec("rtdl") is not None

if HAS_PANDAS:
    import pandas as pd
else:
    pd = None
if HAS_NUMPY:
    import numpy as np
else:
    np = None

if HAS_SKLEARN:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.cluster import DBSCAN, KMeans
    from sklearn.compose import ColumnTransformer
    from sklearn.decomposition import PCA
    from sklearn.ensemble import AdaBoostClassifier, AdaBoostRegressor, ExtraTreesClassifier, ExtraTreesRegressor, HistGradientBoostingClassifier, HistGradientBoostingRegressor, IsolationForest, RandomForestClassifier, RandomForestRegressor, VotingClassifier, VotingRegressor
    from sklearn.feature_selection import VarianceThreshold
    from sklearn.impute import SimpleImputer
    from sklearn.inspection import permutation_importance
    from sklearn.linear_model import ElasticNet, LinearRegression, LogisticRegression
    from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, mean_absolute_error, mean_squared_error, precision_score, r2_score, recall_score
    from sklearn.model_selection import cross_val_score, train_test_split
    from sklearn.naive_bayes import GaussianNB
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.svm import SVC, SVR
    from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

T0 = time.perf_counter()


def ms():
    return int((time.perf_counter() - T0) * 1000)


def out(obj):
    obj.setdefault("meta", {})
    obj["meta"].setdefault("execMs", ms())
    print(json.dumps(obj, ensure_ascii=False, default=lambda v: v.tolist() if hasattr(v, "tolist") else str(v)))


def err(code, message, details=None):
    r = {"ok": False, "code": code, "message": message}
    if details is not None:
        r["details"] = details
    return r


def caps():
    return {
        "ok": True,
        "data": {
            "sklearn": HAS_SKLEARN,
            "pandas": HAS_PANDAS,
            "deepLearningMode": "optional" if HAS_SKLEARN else "disabled",
            "shap": HAS_SHAP,
            "statsmodels": HAS_STATSMODELS,
            "pytorch_tabnet": HAS_PYTORCH_TABNET,
            "rtdl": HAS_RTDL,
            "xgboost": HAS_XGBOOST,
            "lightgbm": HAS_LIGHTGBM,
            "catboost": HAS_CATBOOST,
        },
    }


def metric_contract(task, metrics):
    primary_name = "accuracy" if task == "classification" else "r2"
    if task == "anomaly":
        primary_name = "outlier_ratio"
    if task == "clustering":
        primary_name = "cluster_count"
    if task == "dim_reduction":
        primary_name = "explained_variance_total"
    if task == "timeseries":
        primary_name = "mae"
    primary_value = metrics.get(primary_name)
    return {
        "version": "v1",
        "task": task,
        "primary": {"name": primary_name, "value": primary_value, "goal": "higher" if primary_name not in {"mae", "rmse", "outlier_ratio"} else "lower"},
        "items": [{"name": k, "value": v, "goal": "higher"} for k, v in metrics.items()],
    }


def base_payload(task, model, rows_used, preset, warnings=None):
    return {
        "task": task,
        "model": model,
        "rowsUsed": rows_used,
        "preset": preset,
        "metrics": {},
        "importance": [],
        "featureImportance": [],
        "permutationImportance": [],
        "shapImportance": [],
        "errorAnalysis": {},
        "modelArtifact": None,
        "diagnostics": {},
        "warnings": warnings or [],
        "metricsContract": {"version": "v1", "task": task, "primary": {}, "items": []},
    }


def validate(payload):
    op = str(payload.get("op") or "").strip().lower()
    if op not in {"capabilities", "train"}:
        return err("ML_OP_INVALID", "unsupported op")
    if op == "capabilities":
        return None
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return err("ML_ROWS_REQUIRED", "rows are required")
    task = str(payload.get("task") or "").strip().lower()
    if task not in {"regression", "classification", "anomaly", "clustering", "dim_reduction", "timeseries"}:
        return err("ML_TASK_INVALID", "unsupported task")
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    if task in {"regression", "classification", "timeseries"} and not str(args.get("target") or "").strip():
        return err("ML_TARGET_REQUIRED", "target is required")
    return None


def moving_avg_forecast(payload):
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    opts = payload.get("options") if isinstance(payload.get("options"), dict) else {}
    rows = payload.get("rows") or []
    target = str(args.get("target") or "").strip()
    time_col = str(args.get("timeColumn") or "t").strip()
    window = max(2, int(opts.get("maWindow") or 4))
    horizon = max(1, int(opts.get("horizon") or 6))
    series = []
    for row in rows:
        try:
            series.append((str(row.get(time_col) or len(series)), float(row.get(target))))
        except Exception:
            continue
    if len(series) < window:
        return err("ML_TOO_FEW_ROWS", "not enough rows for moving average")
    values = [v for _, v in series]
    forecast = []
    hist = values[:]
    for idx in range(horizon):
        pred = sum(hist[-window:]) / window
        actual = values[idx] if idx < len(values) else pred
        forecast.append({"t": series[min(idx, len(series) - 1)][0] if idx < len(series) else f"future_{idx+1}", "actual": actual, "predicted": pred})
        hist.append(pred)
    mae = sum(abs(item["actual"] - item["predicted"]) for item in forecast) / len(forecast)
    rmse = math.sqrt(sum((item["actual"] - item["predicted"]) ** 2 for item in forecast) / len(forecast))
    data = base_payload("timeseries", "moving_avg", len(rows), str(opts.get("preset") or "balanced"))
    data["metrics"] = {"mae": mae, "rmse": rmse}
    data["timeSeriesPreview"] = {"forecast": forecast}
    data["metricsContract"] = metric_contract("timeseries", data["metrics"])
    return {"ok": True, "data": data}


def if_dep_missing(model):
    if model == "xgboost" and not HAS_XGBOOST:
        return True
    if model == "lightgbm" and not HAS_LIGHTGBM:
        return True
    if model == "catboost" and not HAS_CATBOOST:
        return True
    if model in {"tabnet"} and not HAS_PYTORCH_TABNET:
        return True
    if model in {"ft_transformer"} and not HAS_RTDL:
        return True
    if model in {"torch_mlp", "tf_mlp"}:
        return True
    return False


def sklearn_train(payload):
    rows = payload.get("rows") or []
    task = str(payload.get("task") or "").strip().lower()
    model = str(payload.get("model") or "").strip().lower()
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    opts = payload.get("options") if isinstance(payload.get("options"), dict) else {}
    if if_dep_missing(model):
        return err("ML_DEP_MISSING", f"optional dependency for '{model}' is missing")
    if not HAS_PANDAS or not HAS_NUMPY:
        return err("ML_PANDAS_REQUIRED", "pandas and numpy are required")

    df = pd.DataFrame(rows)
    target = str(args.get("target") or "").strip()
    feature_cols = [c for c in (args.get("features") or []) if c in df.columns]
    if task != "timeseries" and not feature_cols:
        feature_cols = [c for c in df.columns if c != target][: min(5, max(1, len(df.columns) - 1))]
    if task != "timeseries" and not feature_cols:
        return err("ML_FEATURES_REQUIRED", "features are required")
    preset = str(opts.get("preset") or "balanced")

    if task == "anomaly":
        X = pd.get_dummies(df[feature_cols], dummy_na=False).fillna(0)
        clf = IsolationForest(contamination=opts.get("contamination") if opts.get("contamination") not in {None, "auto"} else "auto", random_state=42)
        labels = clf.fit_predict(X)
        count_out = int((labels == -1).sum())
        data = base_payload(task, model, len(rows), preset)
        data["metrics"] = {"outlier_count": count_out, "outlier_ratio": count_out / len(X)}
        data["anomalySummary"] = {"labelCounts": [{"label": "outlier", "count": count_out}, {"label": "inlier", "count": int((labels == 1).sum())}]}
        data["metricsContract"] = metric_contract(task, data["metrics"])
        return {"ok": True, "data": data}

    if task == "clustering":
        X = pd.get_dummies(df[feature_cols], dummy_na=False).fillna(0)
        clf = KMeans(n_clusters=max(2, int(opts.get("nClusters") or 3)), random_state=42, n_init="auto") if model == "kmeans" else DBSCAN(eps=float(opts.get("eps") or 0.5), min_samples=max(1, int(opts.get("minSamples") or 5)))
        labels = clf.fit_predict(X)
        uniq, counts = np.unique(labels, return_counts=True)
        data = base_payload(task, model, len(rows), preset)
        data["metrics"] = {"cluster_count": int(len(set(labels)))}
        data["clusterSummary"] = [{"cluster": int(k), "count": int(v)} for k, v in zip(uniq.tolist(), counts.tolist())]
        data["metricsContract"] = metric_contract(task, data["metrics"])
        return {"ok": True, "data": data}

    if task == "dim_reduction":
        X = pd.get_dummies(df[feature_cols], dummy_na=False).fillna(0)
        n_comp = max(1, int(opts.get("nComponents") or 2))
        clf = PCA(n_components=n_comp, random_state=42)
        proj = clf.fit_transform(X)
        cols = [f"PC{i+1}" for i in range(proj.shape[1])]
        preview = [{col: float(row[idx]) for idx, col in enumerate(cols)} for row in proj[:20]]
        data = base_payload(task, model, len(rows), preset)
        data["explainedVarianceRatio"] = [float(v) for v in clf.explained_variance_ratio_.tolist()]
        data["projectionPreview"] = preview
        data["metrics"] = {"explained_variance_total": float(sum(data["explainedVarianceRatio"]))}
        data["metricsContract"] = metric_contract(task, data["metrics"])
        return {"ok": True, "data": data}

    work = df[[*feature_cols, target]].dropna()
    if len(work) < 3:
        return err("ML_TOO_FEW_ROWS", "not enough complete rows")
    X = pd.get_dummies(work[feature_cols], dummy_na=False)
    y = work[target]
    test_size = float(opts.get("testSize") or 0.2)
    scoring = str(opts.get("scoring") or "").strip()
    valid_scoring = {"classification": {"", "accuracy", "f1_weighted", "precision_weighted", "recall_weighted"}, "regression": {"", "r2", "neg_mean_absolute_error", "neg_root_mean_squared_error", "neg_mean_squared_error"}}
    if scoring not in valid_scoring.get(task, {""}):
        return err("ML_SCORING_INVALID", "invalid scoring metric")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)

    if task == "regression":
        models = {
            "linear": LinearRegression(),
            "elasticnet": ElasticNet(alpha=float(opts.get("alpha") or 1.0), l1_ratio=float(opts.get("l1Ratio") or 0.5), random_state=42),
            "tree": DecisionTreeRegressor(random_state=42),
            "forest": RandomForestRegressor(random_state=42, n_estimators=200),
            "extra_trees": ExtraTreesRegressor(random_state=42, n_estimators=200),
            "adaboost": AdaBoostRegressor(random_state=42),
            "svm": SVR(C=float(opts.get("svmC") or 1.0), kernel=str(opts.get("svmKernel") or "rbf"), gamma=str(opts.get("svmGamma") or "scale"), epsilon=float(opts.get("svmEpsilon") or 0.1)),
            "voting": VotingRegressor([("linear", LinearRegression()), ("forest", RandomForestRegressor(random_state=42, n_estimators=120))]),
            "hgb": HistGradientBoostingRegressor(random_state=42),
            "nn": MLPRegressor(hidden_layer_sizes=(64, 32), random_state=42, max_iter=300),
        }
    else:
        models = {
            "linear": LogisticRegression(max_iter=2000),
            "tree": DecisionTreeClassifier(random_state=42),
            "forest": RandomForestClassifier(random_state=42, n_estimators=200),
            "extra_trees": ExtraTreesClassifier(random_state=42, n_estimators=200),
            "adaboost": AdaBoostClassifier(random_state=42),
            "svm": SVC(C=float(opts.get("svmC") or 1.0), kernel=str(opts.get("svmKernel") or "rbf"), gamma=str(opts.get("svmGamma") or "scale"), probability=True),
            "calibrated": CalibratedClassifierCV(RandomForestClassifier(random_state=42, n_estimators=150), method=str(opts.get("calibMethod") or "sigmoid"), cv=max(2, int(opts.get("calibCv") or 3))),
            "voting": VotingClassifier([("logit", LogisticRegression(max_iter=2000)), ("forest", RandomForestClassifier(random_state=42, n_estimators=120))], voting="soft"),
            "hgb": HistGradientBoostingClassifier(random_state=42),
            "nb": GaussianNB(),
            "knn": KNeighborsClassifier(n_neighbors=max(1, int(opts.get("nNeighbors") or 5))),
            "nn": MLPClassifier(hidden_layer_sizes=(64, 32), random_state=42, max_iter=300),
        }
    if model not in models:
        return err("ML_MODEL_INVALID", "unsupported model")

    clf = models[model]
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    data = base_payload(task, model, len(rows), preset)
    data["target"] = target
    data["encodedFeatureCount"] = int(X.shape[1])
    if task == "regression":
        resid = y_test - pred
        data["metrics"] = {
            "r2": float(r2_score(y_test, pred)),
            "mae": float(mean_absolute_error(y_test, pred)),
            "rmse": float(math.sqrt(mean_squared_error(y_test, pred))),
        }
        data["errorAnalysis"] = {
            "type": "regression",
            "residualSummary": {
                "mean": float(np.mean(resid)),
                "absMean": float(np.mean(np.abs(resid))),
                "q05": float(np.quantile(resid, 0.05)),
                "q95": float(np.quantile(resid, 0.95)),
            },
            "topResiduals": [{"actual": float(a), "predicted": float(b), "residual": float(a - b)} for a, b in list(zip(y_test.tolist(), pred.tolist()))[:10]],
        }
    else:
        labels = sorted(set(y_test.tolist()) | set(pred.tolist()))
        data["metrics"] = {
            "accuracy": float(accuracy_score(y_test, pred)),
            "precision_weighted": float(precision_score(y_test, pred, average="weighted", zero_division=0)),
            "recall_weighted": float(recall_score(y_test, pred, average="weighted", zero_division=0)),
            "f1_weighted": float(f1_score(y_test, pred, average="weighted", zero_division=0)),
        }
        data["errorAnalysis"] = {"type": "classification", "labels": labels, "matrix": confusion_matrix(y_test, pred, labels=labels).tolist()}

    if hasattr(clf, "feature_importances_"):
        fi = [{"feature": str(col), "importance": float(val)} for col, val in zip(X.columns.tolist(), clf.feature_importances_.tolist())]
        data["importance"] = fi
        data["featureImportance"] = fi
    elif hasattr(clf, "coef_"):
        coef = clf.coef_[0] if hasattr(clf.coef_[0], "__iter__") else clf.coef_
        fi = [{"feature": str(col), "importance": float(abs(val))} for col, val in zip(X.columns.tolist(), coef.tolist())]
        data["importance"] = fi
        data["featureImportance"] = fi

    try:
        perm = permutation_importance(clf, X_test, y_test, n_repeats=5, random_state=42)
        data["permutationImportance"] = [{"feature": str(col), "importance": float(val), "std": float(std)} for col, val, std in zip(X.columns.tolist(), perm.importances_mean.tolist(), perm.importances_std.tolist())][:20]
    except Exception:
        data["permutationImportance"] = []

    cv = max(0, int(opts.get("cv") or 0))
    validation = {"holdoutTestSize": test_size, "scoring": scoring or ("accuracy" if task == "classification" else "r2"), "cv": {"enabled": False}}
    if cv > 1 and len(X) >= cv:
        scores = cross_val_score(clf, X, y, cv=cv, scoring=scoring or None)
        validation["cv"] = {"enabled": True, "folds": cv, "mean": float(np.mean(scores)), "std": float(np.std(scores))}
    data["validation"] = validation
    if opts.get("includeArtifact"):
        blob = pickle.dumps(clf)
        data["modelArtifact"] = {"format": "pickle-base64", "byteSize": len(blob), "payload": base64.b64encode(blob).decode("ascii")}
    data["diagnostics"] = {"trainRows": int(len(X_train)), "testRows": int(len(X_test)), "featureNames": X.columns.tolist()[:50]}
    data["metricsContract"] = metric_contract(task, data["metrics"])
    return {"ok": True, "data": data}


def main():
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError:
        out(err("INVALID_JSON", "invalid json"))
        return
    if not isinstance(payload, dict):
        out(err("INVALID_PAYLOAD", "payload must be an object"))
        return
    validation = validate(payload)
    if validation:
        out(validation)
        return
    op = str(payload.get("op") or "").strip().lower()
    if op == "capabilities":
        out(caps())
        return
    task = str(payload.get("task") or "").strip().lower()
    model = str(payload.get("model") or "").strip().lower()
    if task == "timeseries" and model == "moving_avg":
        out(moving_avg_forecast(payload))
        return
    if not HAS_SKLEARN:
        out(err("ML_SKLEARN_REQUIRED", "sklearn is required for this model"))
        return
    out(sklearn_train(payload))


if __name__ == "__main__":
    main()
