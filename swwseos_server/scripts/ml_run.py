import base64
import importlib.util
import json
import math
import pickle
import sys
import time
import warnings

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
if HAS_STATSMODELS:
    import statsmodels.api as sm
    try:
        from statsmodels.tools.sm_exceptions import PerfectSeparationWarning
    except Exception:
        class PerfectSeparationWarning(Warning):
            pass
else:
    sm = None
    class PerfectSeparationWarning(Warning):
        pass

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
    from sklearn.metrics import silhouette_score
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
            "numpy": HAS_NUMPY,
            "deepLearningMode": "optional" if HAS_SKLEARN else "disabled",
            "shap": HAS_SHAP,
            "statsmodels": HAS_STATSMODELS,
            "pytorch_tabnet": HAS_PYTORCH_TABNET,
            "rtdl": HAS_RTDL,
            "xgboost": HAS_XGBOOST,
            "lightgbm": HAS_LIGHTGBM,
            "catboost": HAS_CATBOOST,
            "fallbackModels": {
                "regression": ["linear"] if HAS_PANDAS and HAS_NUMPY else [],
                "classification": ["linear"] if HAS_PANDAS and HAS_NUMPY else [],
                "clustering": ["kmeans", "dbscan"] if HAS_PANDAS and HAS_NUMPY else [],
                "dim_reduction": ["pca"] if HAS_PANDAS and HAS_NUMPY else [],
            },
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


def split_holdout(X, y, test_size=0.2, random_state=42):
    n = len(X)
    if n < 3:
        return None
    rng = np.random.default_rng(int(random_state))
    idx = np.arange(n)
    rng.shuffle(idx)
    test_n = max(1, min(n - 1, int(round(n * float(test_size)))))
    test_idx = idx[:test_n]
    train_idx = idx[test_n:]
    if len(train_idx) < 2:
        return None
    return (
        X.iloc[train_idx].reset_index(drop=True),
        X.iloc[test_idx].reset_index(drop=True),
        y.iloc[train_idx].reset_index(drop=True),
        y.iloc[test_idx].reset_index(drop=True),
    )


def regression_scores(actual, predicted):
    actual_arr = np.asarray(actual, dtype=float)
    pred_arr = np.asarray(predicted, dtype=float)
    resid = actual_arr - pred_arr
    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((actual_arr - np.mean(actual_arr)) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    mae = float(np.mean(np.abs(resid)))
    rmse = float(np.sqrt(np.mean(resid ** 2)))
    return {"r2": r2, "mae": mae, "rmse": rmse}, resid


def confusion_counts(actual, predicted, labels):
    label_index = {label: idx for idx, label in enumerate(labels)}
    matrix = [[0 for _ in labels] for _ in labels]
    for a, p in zip(actual, predicted):
        matrix[label_index[a]][label_index[p]] += 1
    return matrix


def weighted_classification_scores(actual, predicted):
    actual_labels = [str(v) for v in actual]
    pred_labels = [str(v) for v in predicted]
    labels = sorted(set(actual_labels) | set(pred_labels))
    matrix = confusion_counts(actual_labels, pred_labels, labels)
    total = max(1, len(actual_labels))
    correct = sum(matrix[i][i] for i in range(len(labels)))
    precision_weighted = 0.0
    recall_weighted = 0.0
    f1_weighted = 0.0
    for idx, label in enumerate(labels):
        tp = matrix[idx][idx]
        support = sum(matrix[idx])
        predicted_count = sum(row[idx] for row in matrix)
        precision = tp / predicted_count if predicted_count else 0.0
        recall = tp / support if support else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        weight = support / total
        precision_weighted += precision * weight
        recall_weighted += recall * weight
        f1_weighted += f1 * weight
    return {
        "accuracy": correct / total,
        "precision_weighted": precision_weighted,
        "recall_weighted": recall_weighted,
        "f1_weighted": f1_weighted,
    }, labels, matrix


def fallback_model_artifact(payload):
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return {
        "format": "json-base64",
        "byteSize": len(raw),
        "payload": base64.b64encode(raw).decode("ascii"),
    }


def build_feature_matrix(df, feature_cols):
    X = pd.get_dummies(df[feature_cols], dummy_na=False).fillna(0)
    return X.astype(float)


def pairwise_distance_matrix(arr):
    diffs = arr[:, None, :] - arr[None, :, :]
    return np.sqrt(np.sum(diffs ** 2, axis=2))


def silhouette_score_from_labels(arr, labels, ignore_noise=False):
    labels_arr = np.asarray(labels, dtype=int)
    if ignore_noise:
        mask = labels_arr != -1
        arr = arr[mask]
        labels_arr = labels_arr[mask]
    unique = sorted(set(labels_arr.tolist()))
    if len(unique) < 2 or len(arr) < 2:
        return None
    dist = pairwise_distance_matrix(arr)
    scores = []
    for idx in range(len(arr)):
        same_mask = labels_arr == labels_arr[idx]
        same_mask[idx] = False
        if np.any(same_mask):
            a = float(np.mean(dist[idx][same_mask]))
        else:
            a = 0.0
        b_candidates = []
        for label in unique:
            if label == labels_arr[idx]:
                continue
            other_mask = labels_arr == label
            if np.any(other_mask):
                b_candidates.append(float(np.mean(dist[idx][other_mask])))
        if not b_candidates:
            continue
        b = min(b_candidates)
        denom = max(a, b)
        scores.append((b - a) / denom if denom else 0.0)
    return float(np.mean(scores)) if scores else None


def kmeans_numpy(arr, n_clusters=3, random_state=42, max_iter=60):
    if len(arr) < 2:
        raise ValueError("not enough rows")
    k = max(2, min(int(n_clusters), len(arr)))
    rng = np.random.default_rng(int(random_state))
    init_idx = rng.choice(len(arr), size=k, replace=False)
    centroids = arr[init_idx].astype(float)
    labels = np.zeros(len(arr), dtype=int)
    for _ in range(max_iter):
        distances = np.linalg.norm(arr[:, None, :] - centroids[None, :, :], axis=2)
        next_labels = np.argmin(distances, axis=1)
        next_centroids = centroids.copy()
        for cluster_idx in range(k):
            members = arr[next_labels == cluster_idx]
            if len(members):
                next_centroids[cluster_idx] = np.mean(members, axis=0)
            else:
                farthest_idx = int(np.argmax(np.min(distances, axis=1)))
                next_centroids[cluster_idx] = arr[farthest_idx]
        if np.array_equal(next_labels, labels) and np.allclose(next_centroids, centroids):
            labels = next_labels
            centroids = next_centroids
            break
        labels = next_labels
        centroids = next_centroids
    inertia = float(np.sum((arr - centroids[labels]) ** 2))
    return labels, centroids, inertia


def dbscan_numpy(arr, eps=0.5, min_samples=5):
    if len(arr) < 2:
        raise ValueError("not enough rows")
    eps = float(eps)
    min_samples = max(1, int(min_samples))
    dist = pairwise_distance_matrix(arr)
    labels = np.full(len(arr), -99, dtype=int)
    visited = np.zeros(len(arr), dtype=bool)
    cluster_id = 0

    def neighbors(index):
        return np.where(dist[index] <= eps)[0]

    for index in range(len(arr)):
        if visited[index]:
            continue
        visited[index] = True
        seed_neighbors = neighbors(index)
        if len(seed_neighbors) < min_samples:
            labels[index] = -1
            continue
        labels[index] = cluster_id
        seeds = list(seed_neighbors.tolist())
        ptr = 0
        while ptr < len(seeds):
            point = seeds[ptr]
            ptr += 1
            if not visited[point]:
                visited[point] = True
                point_neighbors = neighbors(point)
                if len(point_neighbors) >= min_samples:
                    for candidate in point_neighbors.tolist():
                        if candidate not in seeds:
                            seeds.append(candidate)
            if labels[point] in {-99, -1}:
                labels[point] = cluster_id
        cluster_id += 1

    labels[labels == -99] = -1
    return labels


def pca_numpy(arr, n_components=2):
    if len(arr) < 2:
        raise ValueError("not enough rows")
    component_count = max(1, min(int(n_components), min(arr.shape[0], arr.shape[1])))
    centered = arr - np.mean(arr, axis=0, keepdims=True)
    u, s, vt = np.linalg.svd(centered, full_matrices=False)
    components = vt[:component_count]
    projection = centered @ components.T
    if len(arr) > 1:
        explained = (s ** 2) / max(1, len(arr) - 1)
    else:
        explained = s ** 2
    total = float(np.sum(explained))
    ratios = (explained[:component_count] / total).tolist() if total else [0.0 for _ in range(component_count)]
    return projection, ratios, components


def fallback_train(payload):
    rows = payload.get("rows") or []
    task = str(payload.get("task") or "").strip().lower()
    requested_model = str(payload.get("model") or "linear").strip().lower() or "linear"
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    opts = payload.get("options") if isinstance(payload.get("options"), dict) else {}
    if task not in {"regression", "classification", "clustering", "dim_reduction"}:
        return err("ML_SKLEARN_REQUIRED", "sklearn is required for this model")
    if not HAS_PANDAS or not HAS_NUMPY:
        return err("ML_PANDAS_REQUIRED", "pandas and numpy are required")

    df = pd.DataFrame(rows)
    feature_cols = [c for c in (args.get("features") or []) if c in df.columns]

    if task in {"clustering", "dim_reduction"}:
        if not feature_cols:
            feature_cols = list(df.columns[: min(5, len(df.columns))])
        if not feature_cols:
            return err("ML_FEATURES_REQUIRED", "features are required")
        work = df[feature_cols].dropna()
        if len(work) < 2:
            return err("ML_TOO_FEW_ROWS", "not enough complete rows")
        X = build_feature_matrix(work, feature_cols)
        arr = X.to_numpy(dtype=float)
        warning_messages = [f"sklearn is unavailable, so the backend used the numpy fallback runtime for '{requested_model}'."]
        data = base_payload(task, requested_model, len(rows), str(opts.get("preset") or "balanced"), warning_messages)
        data["encodedFeatureCount"] = int(X.shape[1])
        data["diagnostics"] = {
            "runtime": "fallback",
            "requestedModel": requested_model,
            "featureNames": X.columns.tolist()[:50],
            "rowsUsed": int(len(work)),
        }
        data["validation"] = {
            "holdoutTestSize": 0.0,
            "scoring": "",
            "cv": {"enabled": False, "reason": "unsupervised fallback runtime does not run cross-validation"},
        }

        if task == "clustering":
            if requested_model == "kmeans":
                labels, centroids, inertia = kmeans_numpy(
                    arr,
                    n_clusters=max(2, int(opts.get("nClusters") or 3)),
                    random_state=int(opts.get("randomState") or 42),
                )
                silhouette = silhouette_score_from_labels(arr, labels.tolist())
                data["metrics"] = {
                    "cluster_count": int(len(sorted(set(labels.tolist())))),
                    "inertia": inertia,
                }
                if silhouette is not None:
                    data["metrics"]["silhouette"] = float(silhouette)
                data["clusterSummary"] = [
                    {"cluster": int(cluster_id), "count": int(count)}
                    for cluster_id, count in zip(*np.unique(labels, return_counts=True))
                ]
                data["clusterSizes"] = list(data["clusterSummary"])
                data["centroidPreview"] = [
                    {str(X.columns[col_idx]): float(value) for col_idx, value in enumerate(center.tolist()[: min(6, len(X.columns))])}
                    for center in centroids[: min(3, len(centroids))]
                ]
            else:
                labels = dbscan_numpy(
                    arr,
                    eps=float(opts.get("eps") or 0.5),
                    min_samples=max(1, int(opts.get("minSamples") or 5)),
                )
                cluster_ids = [cluster_id for cluster_id in sorted(set(labels.tolist())) if cluster_id != -1]
                silhouette = silhouette_score_from_labels(arr, labels.tolist(), ignore_noise=True)
                noise_count = int(np.sum(labels == -1))
                data["metrics"] = {
                    "cluster_count": int(len(cluster_ids)),
                    "noise_ratio": float(noise_count / len(labels)),
                }
                if silhouette is not None:
                    data["metrics"]["silhouette"] = float(silhouette)
                data["clusterSummary"] = [
                    {"cluster": int(cluster_id), "count": int(count)}
                    for cluster_id, count in zip(*np.unique(labels, return_counts=True))
                ]
                data["clusterSizes"] = list(data["clusterSummary"])
            data["metricsContract"] = metric_contract(task, data["metrics"])
            return {"ok": True, "data": data}

        projection, ratios, components = pca_numpy(arr, n_components=max(1, int(opts.get("nComponents") or 2)))
        preview_cols = [f"PC{i+1}" for i in range(projection.shape[1])]
        data["projectionPreview"] = [
            {col: float(row[col_idx]) for col_idx, col in enumerate(preview_cols)}
            for row in projection[:20]
        ]
        data["projectionMetadata"] = {
            "componentCount": int(projection.shape[1]),
            "inputFeatureCount": int(X.shape[1]),
            "sourceRowCount": int(len(work)),
            "featureNames": X.columns.tolist()[:50],
        }
        data["explainedVarianceRatio"] = [float(value) for value in ratios]
        data["metrics"] = {
            "explained_variance_total": float(sum(ratios)),
            "component_count": int(projection.shape[1]),
        }
        data["componentPreview"] = [
            {
                "component": f"PC{idx + 1}",
                "weights": {
                    str(column): float(weight)
                    for column, weight in zip(X.columns.tolist()[:20], components[idx][:20].tolist())
                },
            }
            for idx in range(min(len(components), 3))
        ]
        data["metricsContract"] = metric_contract(task, data["metrics"])
        return {"ok": True, "data": data}

    target = str(args.get("target") or "").strip()
    if target not in df.columns:
        return err("ML_TARGET_REQUIRED", "target is required")
    feature_cols = [c for c in feature_cols if c != target]
    if not feature_cols:
        feature_cols = [c for c in df.columns if c != target][: min(5, max(1, len(df.columns) - 1))]
    if not feature_cols:
        return err("ML_FEATURES_REQUIRED", "features are required")

    work = df[[*feature_cols, target]].dropna()
    if len(work) < 3:
        return err("ML_TOO_FEW_ROWS", "not enough complete rows")

    X = pd.get_dummies(work[feature_cols], dummy_na=False)
    y = work[target]
    split = split_holdout(X, y, float(opts.get("testSize") or 0.2), int(opts.get("randomState") or 42))
    if split is None:
        return err("ML_TOO_FEW_ROWS", "not enough rows after holdout split")
    X_train, X_test, y_train, y_test = split

    warning_messages = []
    actual_model = "linear-fallback"
    if requested_model != "linear":
        warning_messages.append(f"sklearn is unavailable, so '{requested_model}' was downgraded to the linear fallback.")
    else:
        warning_messages.append("sklearn is unavailable, so the linear fallback runtime was used.")

    data = base_payload(task, actual_model, len(rows), str(opts.get("preset") or "balanced"), warning_messages)
    data["target"] = target
    data["encodedFeatureCount"] = int(X.shape[1])
    data["diagnostics"] = {
        "trainRows": int(len(X_train)),
        "testRows": int(len(X_test)),
        "featureNames": X.columns.tolist()[:50],
        "runtime": "fallback",
        "requestedModel": requested_model,
    }
    data["validation"] = {
        "holdoutTestSize": float(opts.get("testSize") or 0.2),
        "scoring": str(opts.get("scoring") or ("accuracy" if task == "classification" else "r2")),
        "cv": {"enabled": False, "reason": "fallback runtime does not run cross-validation"},
    }

    if task == "regression":
        X_train_const = np.column_stack([np.ones(len(X_train)), X_train.to_numpy(dtype=float)])
        X_test_const = np.column_stack([np.ones(len(X_test)), X_test.to_numpy(dtype=float)])
        coef, *_ = np.linalg.lstsq(X_train_const, y_train.to_numpy(dtype=float), rcond=None)
        pred = X_test_const @ coef
        metrics, resid = regression_scores(y_test.to_numpy(dtype=float), pred)
        data["metrics"] = metrics
        data["errorAnalysis"] = {
            "type": "regression",
            "residualSummary": {
                "mean": float(np.mean(resid)),
                "absMean": float(np.mean(np.abs(resid))),
                "q05": float(np.quantile(resid, 0.05)),
                "q95": float(np.quantile(resid, 0.95)),
            },
            "topResiduals": [
                {
                    "actual": float(a),
                    "predicted": float(b),
                    "residual": float(a - b),
                }
                for a, b in list(zip(y_test.to_numpy(dtype=float).tolist(), pred.tolist()))[:10]
            ],
        }
        feature_importance = [
            {"feature": str(col), "importance": float(abs(val))}
            for col, val in zip(X.columns.tolist(), coef[1:].tolist())
        ]
        data["importance"] = feature_importance
        data["featureImportance"] = feature_importance
        if opts.get("includeArtifact"):
            data["modelArtifact"] = fallback_model_artifact(
                {
                    "runtime": "linear-fallback",
                    "intercept": float(coef[0]),
                    "coefficients": {
                        str(col): float(val) for col, val in zip(X.columns.tolist(), coef[1:].tolist())
                    },
                }
            )
        data["metricsContract"] = metric_contract(task, data["metrics"])
        return {"ok": True, "data": data}

    classes = sorted({str(v) for v in y_train.tolist()} | {str(v) for v in y_test.tolist()})
    if HAS_STATSMODELS and len(classes) == 2:
        class_to_int = {classes[0]: 0, classes[1]: 1}
        y_train_bin = y_train.map(lambda v: class_to_int[str(v)]).astype(float)
        y_test_bin = y_test.map(lambda v: class_to_int[str(v)]).astype(float)
        X_train_const = sm.add_constant(X_train.astype(float), has_constant="add")
        X_test_const = sm.add_constant(X_test.astype(float), has_constant="add")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=PerfectSeparationWarning)
                warnings.simplefilter("ignore", category=RuntimeWarning)
                fit = sm.Logit(y_train_bin, X_train_const).fit(disp=0, maxiter=200)
            pred_prob = fit.predict(X_test_const)
            pred_bin = (pred_prob >= 0.5).astype(int)
            pred_labels = [classes[int(v)] for v in pred_bin.tolist()]
            feature_importance = [
                {"feature": str(col), "importance": float(abs(val))}
                for col, val in fit.params.drop("const", errors="ignore").items()
            ]
            if opts.get("includeArtifact"):
                data["modelArtifact"] = fallback_model_artifact(
                    {
                        "runtime": "logit-fallback",
                        "params": {str(k): float(v) for k, v in fit.params.items()},
                        "classes": classes,
                    }
                )
            data["warnings"].append("Binary classification used a statsmodels logistic fallback because sklearn is unavailable.")
            actual_model = "logit-fallback"
        except Exception as exc:
            pred_labels = [classes[0] for _ in range(len(y_test))]
            feature_importance = []
            data["warnings"].append(f"statsmodels logistic fallback failed ({exc}); majority-class fallback was used instead.")
            actual_model = "majority-fallback"
    else:
        majority = y_train.astype(str).mode().iloc[0]
        pred_labels = [str(majority) for _ in range(len(y_test))]
        feature_importance = []
        actual_model = "majority-fallback"
        if len(classes) > 2:
            data["warnings"].append("Multiclass fallback used majority-class predictions because sklearn is unavailable.")
        else:
            data["warnings"].append("Binary classification fallback used majority-class predictions because statsmodels is unavailable.")

    metrics, labels, matrix = weighted_classification_scores(y_test.astype(str).tolist(), pred_labels)
    data["model"] = actual_model
    data["metrics"] = metrics
    data["errorAnalysis"] = {
        "type": "classification",
        "labels": labels,
        "matrix": matrix,
    }
    data["importance"] = feature_importance
    data["featureImportance"] = feature_importance
    data["metricsContract"] = metric_contract(task, data["metrics"])
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
        metrics = {"cluster_count": int(len([cluster_id for cluster_id in uniq.tolist() if cluster_id != -1]))}
        if model == "kmeans" and hasattr(clf, "inertia_"):
            metrics["inertia"] = float(clf.inertia_)
        try:
            silhouette = silhouette_score(X, labels) if len(set(labels.tolist())) > 1 and len(X) > len(set(labels.tolist())) else None
        except Exception:
            silhouette = None
        if silhouette is not None:
            metrics["silhouette"] = float(silhouette)
        if -1 in uniq.tolist():
            metrics["noise_ratio"] = float((labels == -1).sum() / len(labels))
        data["metrics"] = metrics
        data["clusterSummary"] = [{"cluster": int(k), "count": int(v)} for k, v in zip(uniq.tolist(), counts.tolist())]
        data["clusterSizes"] = list(data["clusterSummary"])
        data["diagnostics"] = {"runtime": "backend", "featureNames": X.columns.tolist()[:50], "rowsUsed": int(len(X))}
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
        data["projectionMetadata"] = {
            "componentCount": int(proj.shape[1]),
            "inputFeatureCount": int(X.shape[1]),
            "sourceRowCount": int(len(X)),
            "featureNames": X.columns.tolist()[:50],
        }
        data["metrics"] = {
            "explained_variance_total": float(sum(data["explainedVarianceRatio"])),
            "component_count": int(proj.shape[1]),
        }
        data["diagnostics"] = {"runtime": "backend", "featureNames": X.columns.tolist()[:50], "rowsUsed": int(len(X))}
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
        if task in {"regression", "classification", "clustering", "dim_reduction"}:
            out(fallback_train(payload))
            return
        out(err("ML_SKLEARN_REQUIRED", "sklearn is required for this model"))
        return
    out(sklearn_train(payload))


if __name__ == "__main__":
    main()
