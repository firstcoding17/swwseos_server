import json
import math
import sys
from typing import Any, Dict, List


def emit(obj: Dict[str, Any]) -> None:
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def fail(code: str, message: str, details: Any = None) -> None:
    payload = {"ok": False, "code": code, "message": message}
    if details is not None:
        payload["details"] = details
    emit(payload)


def normalize_success_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return payload
    data = payload.get("data")
    if not isinstance(data, dict):
        return payload

    if "metrics" not in data or not isinstance(data.get("metrics"), dict):
        data["metrics"] = {}
    if "warnings" not in data or not isinstance(data.get("warnings"), list):
        data["warnings"] = []

    if "importance" not in data and isinstance(data.get("featureImportance"), list):
        data["importance"] = data.get("featureImportance")
    if "featureImportance" not in data and isinstance(data.get("importance"), list):
        data["featureImportance"] = data.get("importance")
    if "importance" not in data:
        data["importance"] = []
    if "permutationImportance" not in data or not isinstance(data.get("permutationImportance"), list):
        data["permutationImportance"] = []
    if "shapImportance" not in data or not isinstance(data.get("shapImportance"), list):
        data["shapImportance"] = []
    if "errorAnalysis" not in data or not isinstance(data.get("errorAnalysis"), dict):
        data["errorAnalysis"] = {}
    if "modelArtifact" not in data or (data.get("modelArtifact") is not None and not isinstance(data.get("modelArtifact"), dict)):
        data["modelArtifact"] = None

    if "diagnostics" not in data or not isinstance(data.get("diagnostics"), dict):
        diagnostics: Dict[str, Any] = {}
        if isinstance(data.get("validation"), dict):
            diagnostics["validation"] = data.get("validation")
        if isinstance(data.get("errorAnalysis"), dict) and data.get("errorAnalysis"):
            diagnostics["errorAnalysis"] = data.get("errorAnalysis")
        if isinstance(data.get("timeSeriesPreview"), dict):
            diagnostics["timeSeries"] = data.get("timeSeriesPreview")
        if isinstance(data.get("anomalySummary"), dict):
            diagnostics["anomaly"] = data.get("anomalySummary")
        if isinstance(data.get("clusterSummary"), list):
            diagnostics["clustering"] = {"clusterSummary": data.get("clusterSummary")}
        dim_diag = {}
        if isinstance(data.get("explainedVarianceRatio"), list):
            dim_diag["explainedVarianceRatio"] = data.get("explainedVarianceRatio")
        if isinstance(data.get("componentLoadings"), list):
            dim_diag["componentLoadings"] = data.get("componentLoadings")
        if isinstance(data.get("projectionPreview"), list):
            dim_diag["projectionPreview"] = data.get("projectionPreview")
        if dim_diag:
            diagnostics["dimReduction"] = dim_diag
        data["diagnostics"] = diagnostics

    if "metricsContract" not in data or not isinstance(data.get("metricsContract"), dict):
        task = str(data.get("task") or "unknown")
        metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}

        primary_candidates = {
            "regression": ["r2", "rmse", "mae", "neg_root_mean_squared_error"],
            "classification": ["accuracy", "f1_weighted", "precision_weighted", "recall_weighted"],
            "clustering": ["silhouette", "cluster_count", "noise_count"],
            "anomaly": ["outlier_ratio", "score_mean", "outlier_count"],
            "dim_reduction": ["explained_variance_total", "components"],
            "timeseries": ["mae", "rmse", "mape"],
        }
        lower_is_better = {
            "mae",
            "mse",
            "rmse",
            "mape",
            "neg_mean_absolute_error",
            "neg_mean_squared_error",
            "neg_root_mean_squared_error",
            "outlier_count",
            "outlier_ratio",
            "noise_count",
        }

        def as_metric_number(v: Any) -> Any:
            try:
                if isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        primary_name = None
        for key in primary_candidates.get(task, []):
            if as_metric_number(metrics.get(key)) is not None:
                primary_name = key
                break
        if primary_name is None:
            for key, val in metrics.items():
                if as_metric_number(val) is not None:
                    primary_name = str(key)
                    break

        metric_items = []
        for key, val in metrics.items():
            num = as_metric_number(val)
            if num is None:
                continue
            metric_items.append(
                {
                    "name": str(key),
                    "value": num,
                    "goal": "lower" if str(key) in lower_is_better else "higher",
                }
            )

        primary = None
        if primary_name is not None:
            primary_num = as_metric_number(metrics.get(primary_name))
            if primary_num is not None:
                primary = {
                    "name": primary_name,
                    "value": primary_num,
                    "goal": "lower" if primary_name in lower_is_better else "higher",
                }

        data["metricsContract"] = {
            "version": "v1",
            "task": task,
            "primary": primary,
            "items": metric_items,
        }

    return payload


def check_capabilities() -> Dict[str, Any]:
    try:
        import importlib.util as ilu

        has_sklearn = bool(ilu.find_spec("sklearn"))
        has_pandas = bool(ilu.find_spec("pandas"))
        has_numpy = bool(ilu.find_spec("numpy"))
        has_torch = bool(ilu.find_spec("torch"))
        has_tf = bool(ilu.find_spec("tensorflow"))
        has_pytorch_tabnet = bool(ilu.find_spec("pytorch_tabnet"))
        has_rtdl = bool(ilu.find_spec("rtdl"))
        has_shap = bool(ilu.find_spec("shap"))
        has_statsmodels = bool(ilu.find_spec("statsmodels"))
        has_xgboost = bool(ilu.find_spec("xgboost"))
        has_lightgbm = bool(ilu.find_spec("lightgbm"))
        has_catboost = bool(ilu.find_spec("catboost"))
        return {
            "ok": True,
            "data": {
                "sklearn": has_sklearn,
                "pandas": has_pandas,
                "numpy": has_numpy,
                "torch": has_torch,
                "tensorflow": has_tf,
                "pytorch_tabnet": has_pytorch_tabnet,
                "rtdl": has_rtdl,
                "shap": has_shap,
                "statsmodels": has_statsmodels,
                "xgboost": has_xgboost,
                "lightgbm": has_lightgbm,
                "catboost": has_catboost,
                "deepLearningMode": "nn-sklearn",
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "code": "ML_CAPABILITY_EXCEPTION",
            "message": "ml capability check failed",
            "details": str(e),
        }


def feature_importance_map(model: Any, feature_names: List[str]) -> List[Dict[str, Any]]:
    pairs = []
    if hasattr(model, "feature_importances_"):
        vals = list(model.feature_importances_)
        for name, val in zip(feature_names, vals):
            pairs.append({"feature": name, "importance": float(val)})
    elif hasattr(model, "coef_"):
        coef = model.coef_
        if hasattr(coef, "ndim") and getattr(coef, "ndim", 1) > 1:
            abs_mean = (abs(coef)).mean(axis=0)
            vals = list(abs_mean.tolist())
        else:
            vals = list(abs(coef).tolist())
        for name, val in zip(feature_names, vals):
            pairs.append({"feature": name, "importance": float(val)})
    pairs.sort(key=lambda x: x["importance"], reverse=True)
    return pairs[:20]


def permutation_importance_map(
    model: Any,
    X_eval: Any,
    y_eval: Any,
    feature_names: List[str],
    scoring: str,
    random_state: int,
) -> List[Dict[str, Any]]:
    from sklearn.inspection import permutation_importance

    if not hasattr(X_eval, "shape"):
        return []
    row_count = int(X_eval.shape[0])
    col_count = int(X_eval.shape[1])
    if row_count < 8 or col_count < 1:
        return []

    result = permutation_importance(
        model,
        X_eval,
        y_eval,
        scoring=scoring,
        n_repeats=5 if row_count <= 1200 else 3,
        random_state=random_state,
    )
    vals = list(result.importances_mean.tolist())
    stds = list(result.importances_std.tolist())
    pairs = []
    for name, val, std in zip(feature_names, vals, stds):
        pairs.append({"feature": name, "importance": float(val), "std": float(std)})
    pairs.sort(key=lambda x: abs(x["importance"]), reverse=True)
    return pairs[:20]


def create_model_artifact(
    model: Any,
    task: str,
    model_kind: str,
    target: Any,
    feature_names: List[str],
    preset: str,
) -> Dict[str, Any]:
    import base64
    import hashlib
    import pickle

    blob = pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)
    return {
        "format": "pickle-base64",
        "task": task,
        "model": model_kind,
        "target": target,
        "featureNames": feature_names,
        "preset": preset,
        "byteSize": int(len(blob)),
        "sha256": hashlib.sha256(blob).hexdigest(),
        "bytes": base64.b64encode(blob).decode("ascii"),
    }


def autoencoder_importance_map(model: Any, feature_names: List[str]) -> List[Dict[str, Any]]:
    try:
        coefs = getattr(model, "coefs_", None)
        if not coefs or not isinstance(coefs, list) or not coefs[0].size:
            return []
        first_layer = coefs[0]
        pairs = []
        for idx, name in enumerate(feature_names):
            imp = float(sum(abs(float(v)) for v in first_layer[idx]) / max(1, len(first_layer[idx])))
            pairs.append({"feature": name, "importance": imp})
        pairs.sort(key=lambda x: x["importance"], reverse=True)
        return pairs[:20]
    except Exception:
        return []


def shap_importance_map(model: Any, X_eval: Any, feature_names: List[str]) -> List[Dict[str, Any]]:
    import numpy as np
    import shap

    if not hasattr(X_eval, "shape") or int(X_eval.shape[0]) < 2:
        return []
    sample_n = min(int(X_eval.shape[0]), 300)
    sample = X_eval.iloc[:sample_n] if hasattr(X_eval, "iloc") else X_eval[:sample_n]

    explainer = shap.Explainer(model, sample)
    sv = explainer(sample)
    vals = np.array(sv.values)
    if vals.ndim == 3:
        # multiclass: (rows, classes, features)
        agg = np.mean(np.abs(vals), axis=(0, 1))
    elif vals.ndim == 2:
        # regression/binary: (rows, features)
        agg = np.mean(np.abs(vals), axis=0)
    else:
        return []

    pairs = []
    for name, val in zip(feature_names, agg.tolist()):
        pairs.append({"feature": str(name), "importance": float(val)})
    pairs.sort(key=lambda x: x["importance"], reverse=True)
    return pairs[:20]


def train_deep_backend(
    backend_kind: str,
    task: str,
    X_train: Any,
    X_test: Any,
    y_train: Any,
    y_test: Any,
    random_state: int,
    preset: str,
) -> Dict[str, Any]:
    epochs_map = {"fast": 80, "balanced": 140, "accurate": 220}
    epochs = epochs_map.get(preset, 140)

    if backend_kind == "tabnet":
        try:
            import numpy as np
            from pytorch_tabnet.tab_model import TabNetClassifier, TabNetRegressor  # type: ignore
        except Exception:
            return {
                "ok": False,
                "code": "ML_DEP_MISSING",
                "message": "pytorch_tabnet is not installed",
                "details": {"model": backend_kind, "dependency": "pytorch_tabnet"},
            }

        xtr = X_train.to_numpy(dtype=np.float32)
        xte = X_test.to_numpy(dtype=np.float32)

        if task == "regression":
            ytr = np.asarray([float(v) for v in list(y_train)], dtype=np.float32).reshape(-1, 1)
            model = TabNetRegressor(seed=random_state, verbose=0)
            model.fit(xtr, ytr, max_epochs=epochs, patience=20, batch_size=1024, virtual_batch_size=128)
            pred = [float(v) for v in np.asarray(model.predict(xte)).reshape(-1).tolist()]
        else:
            labels = [str(v) for v in list(y_train)]
            classes = sorted(set(labels))
            index_map = {k: i for i, k in enumerate(classes)}
            ytr = np.asarray([index_map[v] for v in labels], dtype=np.int64)
            model = TabNetClassifier(seed=random_state, verbose=0)
            model.fit(xtr, ytr, max_epochs=epochs, patience=20, batch_size=1024, virtual_batch_size=128)
            pred_idx = np.asarray(model.predict(xte)).reshape(-1).tolist()
            pred = [classes[int(i)] if int(i) < len(classes) else classes[0] for i in pred_idx]

        feature_importance = []
        try:
            vals = list(getattr(model, "feature_importances_", []) or [])
            feature_importance = [
                {"feature": name, "importance": float(val)}
                for name, val in zip(list(X_train.columns), vals)
            ]
            feature_importance.sort(key=lambda x: x["importance"], reverse=True)
            feature_importance = feature_importance[:20]
        except Exception:
            feature_importance = []

        return {
            "ok": True,
            "pred": pred,
            "featureImportance": feature_importance,
            "artifactObj": model,
            "warnings": [],
        }

    if backend_kind in {"torch_mlp", "ft_transformer"}:
        try:
            import numpy as np
            import torch
            import torch.nn as nn
        except Exception:
            return {
                "ok": False,
                "code": "ML_DEP_MISSING",
                "message": "torch is not installed",
                "details": {"model": backend_kind, "dependency": "torch"},
            }

        torch.manual_seed(int(random_state))
        xtr = torch.tensor(X_train.to_numpy(dtype=np.float32))
        xte = torch.tensor(X_test.to_numpy(dtype=np.float32))
        in_dim = int(xtr.shape[1])
        hidden = max(16, min(256, in_dim * 4))
        warnings = []
        if backend_kind == "ft_transformer":
            warnings.append("ft_transformer runs in lightweight torch parity mode")

        if task == "regression":
            ytr = torch.tensor([float(v) for v in list(y_train)], dtype=torch.float32).view(-1, 1)
            yte = torch.tensor([float(v) for v in list(y_test)], dtype=torch.float32).view(-1, 1)
            net = nn.Sequential(
                nn.Linear(in_dim, hidden),
                nn.LayerNorm(hidden),
                nn.ReLU(),
                nn.Linear(hidden, max(8, hidden // 2)),
                nn.ReLU(),
                nn.Linear(max(8, hidden // 2), 1),
            )
            loss_fn = nn.MSELoss()
        else:
            labels = [str(v) for v in list(y_train)]
            classes = sorted(set(labels))
            index_map = {k: i for i, k in enumerate(classes)}
            ytr = torch.tensor([index_map[v] for v in labels], dtype=torch.long)
            yte = torch.tensor([index_map.get(str(v), 0) for v in list(y_test)], dtype=torch.long)
            out_dim = max(2, len(classes))
            net = nn.Sequential(
                nn.Linear(in_dim, hidden),
                nn.LayerNorm(hidden),
                nn.ReLU(),
                nn.Linear(hidden, max(8, hidden // 2)),
                nn.ReLU(),
                nn.Linear(max(8, hidden // 2), out_dim),
            )
            loss_fn = nn.CrossEntropyLoss()

        opt = torch.optim.Adam(net.parameters(), lr=1e-3)
        best_state = None
        best_val = None
        patience = 15
        stale = 0
        for _ in range(int(epochs)):
            net.train()
            opt.zero_grad()
            out = net(xtr)
            loss = loss_fn(out, ytr)
            loss.backward()
            opt.step()

            net.eval()
            with torch.no_grad():
                val_out = net(xte)
                val_loss = float(loss_fn(val_out, yte).item())
            if best_val is None or val_loss < best_val:
                best_val = val_loss
                best_state = {k: v.detach().clone() for k, v in net.state_dict().items()}
                stale = 0
            else:
                stale += 1
                if stale >= patience:
                    break

        if best_state is not None:
            net.load_state_dict(best_state)
        net.eval()
        with torch.no_grad():
            logits = net(xte)
        if task == "regression":
            pred = [float(v) for v in logits.view(-1).tolist()]
        else:
            pred_idx = logits.argmax(dim=1).tolist()
            pred = [classes[int(i)] if int(i) < len(classes) else classes[0] for i in pred_idx]

        feature_importance = []
        try:
            first_w = net[0].weight.detach().cpu().numpy()
            vals = np.mean(np.abs(first_w), axis=0).tolist()
            feature_importance = [
                {"feature": name, "importance": float(val)}
                for name, val in zip(list(X_train.columns), vals)
            ]
            feature_importance.sort(key=lambda x: x["importance"], reverse=True)
            feature_importance = feature_importance[:20]
        except Exception:
            feature_importance = []

        return {
            "ok": True,
            "pred": pred,
            "featureImportance": feature_importance,
            "artifactObj": net,
            "warnings": warnings,
        }

    if backend_kind == "tf_mlp":
        try:
            import numpy as np
            import tensorflow as tf
        except Exception:
            return {
                "ok": False,
                "code": "ML_DEP_MISSING",
                "message": "tensorflow is not installed",
                "details": {"model": backend_kind, "dependency": "tensorflow"},
            }

        tf.random.set_seed(int(random_state))
        xtr = X_train.to_numpy(dtype=np.float32)
        xte = X_test.to_numpy(dtype=np.float32)
        in_dim = int(xtr.shape[1])
        hidden = max(16, min(256, in_dim * 4))
        early = tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=12, restore_best_weights=True)

        if task == "regression":
            ytr = np.asarray([float(v) for v in list(y_train)], dtype=np.float32)
            yte = np.asarray([float(v) for v in list(y_test)], dtype=np.float32)
            model = tf.keras.Sequential(
                [
                    tf.keras.layers.Input(shape=(in_dim,)),
                    tf.keras.layers.Dense(hidden, activation="relu"),
                    tf.keras.layers.Dense(max(8, hidden // 2), activation="relu"),
                    tf.keras.layers.Dense(1),
                ]
            )
            model.compile(optimizer="adam", loss="mse")
            model.fit(xtr, ytr, validation_data=(xte, yte), epochs=int(epochs), batch_size=64, verbose=0, callbacks=[early])
            pred = [float(v) for v in model.predict(xte, verbose=0).reshape(-1).tolist()]
        else:
            labels = [str(v) for v in list(y_train)]
            classes = sorted(set(labels))
            index_map = {k: i for i, k in enumerate(classes)}
            ytr = np.asarray([index_map[v] for v in labels], dtype=np.int64)
            out_dim = max(2, len(classes))
            model = tf.keras.Sequential(
                [
                    tf.keras.layers.Input(shape=(in_dim,)),
                    tf.keras.layers.Dense(hidden, activation="relu"),
                    tf.keras.layers.Dense(max(8, hidden // 2), activation="relu"),
                    tf.keras.layers.Dense(out_dim, activation="softmax"),
                ]
            )
            model.compile(optimizer="adam", loss="sparse_categorical_crossentropy")
            model.fit(xtr, ytr, validation_split=0.2, epochs=int(epochs), batch_size=64, verbose=0, callbacks=[early])
            probs = model.predict(xte, verbose=0)
            pred_idx = np.argmax(probs, axis=1).tolist()
            pred = [classes[int(i)] if int(i) < len(classes) else classes[0] for i in pred_idx]

        feature_importance = []
        try:
            first = model.layers[0].get_weights()[0]
            vals = np.mean(np.abs(first), axis=1).tolist()
            feature_importance = [
                {"feature": name, "importance": float(val)}
                for name, val in zip(list(X_train.columns), vals)
            ]
            feature_importance.sort(key=lambda x: x["importance"], reverse=True)
            feature_importance = feature_importance[:20]
        except Exception:
            feature_importance = []

        return {
            "ok": True,
            "pred": pred,
            "featureImportance": feature_importance,
            "artifactObj": model,
            "warnings": [],
        }

    return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {backend_kind}"}


def as_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return math.nan


def as_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def as_ratio(v: Any, default: float) -> float:
    try:
        val = float(v)
        if 0 < val < 1:
            return val
    except Exception:
        pass
    return default


def normalize_preset(v: Any) -> str:
    preset = str(v or "balanced").strip().lower()
    if preset in {"fast", "balanced", "accurate"}:
        return preset
    return "balanced"


def train(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = payload.get("rows") or []
    args = payload.get("args") or {}
    opts = payload.get("options") or {}
    task = str(payload.get("task") or "regression").strip().lower().replace("-", "_")
    model_kind = str(payload.get("model") or "linear").strip().lower()

    if not isinstance(rows, list) or not rows:
        return {"ok": False, "code": "ML_ROWS_REQUIRED", "message": "rows are required"}

    if not isinstance(args, dict):
        args = {}
    if not isinstance(opts, dict):
        opts = {}
    preset_raw = opts.get("preset", "balanced")
    preset = normalize_preset(preset_raw)
    preset_input_norm = str(preset_raw or "").strip().lower()
    invalid_preset = preset_input_norm not in {"fast", "balanced", "accurate"}

    task_alias = {
        "outlier": "anomaly",
        "outlier_detection": "anomaly",
        "dimensionality_reduction": "dim_reduction",
        "dimension_reduction": "dim_reduction",
        "time_series": "timeseries",
        "forecast": "timeseries",
    }
    task = task_alias.get(task, task)

    supported_tasks = {"regression", "classification", "anomaly", "clustering", "dim_reduction", "timeseries"}
    if task not in supported_tasks:
        return {"ok": False, "code": "ML_TASK_INVALID", "message": f"unsupported task: {task}"}

    supervised = task in {"regression", "classification", "timeseries"}
    target = args.get("target")
    features = args.get("features")
    if supervised and not target:
        return {"ok": False, "code": "ML_TARGET_REQUIRED", "message": "args.target is required"}

    try:
        import pandas as pd
        from sklearn.cluster import DBSCAN, KMeans
        from sklearn.decomposition import PCA
        from sklearn.ensemble import (
            AdaBoostClassifier,
            AdaBoostRegressor,
            ExtraTreesClassifier,
            ExtraTreesRegressor,
            VotingClassifier,
            VotingRegressor,
            RandomForestClassifier,
            RandomForestRegressor,
        )
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, IsolationForest
        from sklearn.linear_model import ElasticNet, LinearRegression, LogisticRegression
        from sklearn.metrics import (
            accuracy_score,
            confusion_matrix,
            f1_score,
            mean_absolute_error,
            mean_squared_error,
            precision_score,
            r2_score,
            recall_score,
            silhouette_score,
        )
        from sklearn.model_selection import KFold, StratifiedKFold, cross_val_score, train_test_split
        from sklearn.naive_bayes import GaussianNB
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.neural_network import MLPClassifier, MLPRegressor
        from sklearn.svm import SVC, SVR
        from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
    except Exception as e:
        return {
            "ok": False,
            "code": "ML_SKLEARN_REQUIRED",
            "message": "sklearn/pandas are required for ml run",
            "details": str(e),
        }

    XGBRegressor = None
    XGBClassifier = None
    LGBMRegressor = None
    LGBMClassifier = None
    CatBoostRegressor = None
    CatBoostClassifier = None
    try:
        from xgboost import XGBClassifier, XGBRegressor  # type: ignore
    except Exception:
        pass
    try:
        from lightgbm import LGBMClassifier, LGBMRegressor  # type: ignore
    except Exception:
        pass
    try:
        from catboost import CatBoostClassifier, CatBoostRegressor  # type: ignore
    except Exception:
        pass

    df = pd.DataFrame(rows)
    if supervised and target not in df.columns:
        return {
            "ok": False,
            "code": "ML_TARGET_NOT_FOUND",
            "message": f"target column not found: {target}",
        }

    if task == "timeseries":
        warnings = []
        time_col = args.get("timeColumn")
        horizon = as_int(opts.get("horizon", 12), 12)
        if horizon < 1:
            horizon = 1

        ts_df = df.copy()
        ts_df["_ts_y"] = ts_df[target].map(as_float)
        if time_col and time_col in ts_df.columns:
            ts_df["_ts_time"] = pd.to_datetime(ts_df[time_col], errors="coerce")
            ts_df = ts_df.dropna(subset=["_ts_time"])
            ts_df = ts_df.sort_values("_ts_time")
        else:
            if time_col:
                warnings.append("time column not found; using row order index")
            ts_df["_ts_time"] = list(range(len(ts_df)))

        ts_df = ts_df.dropna(subset=["_ts_y"])
        if len(ts_df) < 12:
            return {
                "ok": False,
                "code": "ML_TOO_FEW_ROWS",
                "message": "timeseries needs at least 12 valid rows",
            }
        if horizon >= len(ts_df):
            horizon = max(1, len(ts_df) // 3)
            warnings.append("horizon was too large and was adjusted automatically")

        train_y = ts_df["_ts_y"].iloc[:-horizon]
        test_y = ts_df["_ts_y"].iloc[-horizon:]
        pred = []
        model_artifact_obj = None

        if model_kind in {"naive", "seasonal_naive"}:
            last_val = float(train_y.iloc[-1])
            pred = [last_val] * horizon
            model_artifact_obj = {"kind": "naive", "lastValue": last_val}
        elif model_kind in {"moving_avg", "moving_average"}:
            window = as_int(opts.get("maWindow", 7), 7)
            if window < 2:
                window = 2
            if window > len(train_y):
                window = len(train_y)
            series = [float(v) for v in train_y.tolist()]
            pred = []
            for _ in range(horizon):
                next_v = float(sum(series[-window:]) / max(1, window))
                pred.append(next_v)
                series.append(next_v)
            model_artifact_obj = {"kind": "moving_avg", "window": window}
        elif model_kind == "arima":
            try:
                from statsmodels.tsa.arima.model import ARIMA  # type: ignore
            except Exception:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "statsmodels is not installed",
                    "details": {"model": model_kind, "dependency": "statsmodels"},
                }
            order_raw = opts.get("arimaOrder", [1, 1, 1])
            try:
                order = tuple(int(v) for v in order_raw[:3])
                if len(order) != 3:
                    order = (1, 1, 1)
            except Exception:
                order = (1, 1, 1)
            fitted = ARIMA(train_y, order=order).fit()
            pred = [float(v) for v in fitted.forecast(steps=horizon).tolist()]
            model_artifact_obj = fitted
        elif model_kind in {"exp_smoothing", "exponential_smoothing", "holt_winters"}:
            try:
                from statsmodels.tsa.holtwinters import ExponentialSmoothing  # type: ignore
            except Exception:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "statsmodels is not installed",
                    "details": {"model": model_kind, "dependency": "statsmodels"},
                }
            seasonal_period = as_int(opts.get("seasonalPeriod", 0), 0)
            trend_raw = str(opts.get("tsTrend", "add") or "add").lower()
            trend = trend_raw if trend_raw in {"add", "mul"} else None
            seasonal = "add" if seasonal_period >= 2 else None
            fitted = ExponentialSmoothing(
                train_y,
                trend=trend,
                seasonal=seasonal,
                seasonal_periods=seasonal_period if seasonal else None,
            ).fit(optimized=True)
            pred = [float(v) for v in fitted.forecast(horizon).tolist()]
            model_artifact_obj = fitted
        else:
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}

        actual = [float(v) for v in test_y.tolist()]
        if len(pred) < len(actual):
            pred.extend([pred[-1] if pred else actual[-1]] * (len(actual) - len(pred)))
        if len(pred) > len(actual):
            pred = pred[: len(actual)]

        abs_err = [abs(a - b) for a, b in zip(actual, pred)]
        sq_err = [(a - b) ** 2 for a, b in zip(actual, pred)]
        mape_terms = [abs((a - b) / a) for a, b in zip(actual, pred) if abs(a) > 1e-9]

        residuals = [a - b for a, b in zip(actual, pred)]
        sorted_res = sorted(residuals)
        q05_idx = max(0, min(len(sorted_res) - 1, int(round((len(sorted_res) - 1) * 0.05))))
        q95_idx = max(0, min(len(sorted_res) - 1, int(round((len(sorted_res) - 1) * 0.95))))

        history_tail = []
        for _, row in ts_df.tail(min(20, len(ts_df))).iterrows():
            history_tail.append({"t": str(row["_ts_time"]), "y": float(row["_ts_y"])})
        test_times = ts_df["_ts_time"].iloc[-len(actual):].tolist()
        forecast_rows = []
        for i, value in enumerate(pred):
            forecast_rows.append(
                {
                    "t": str(test_times[i]) if i < len(test_times) else str(i),
                    "actual": float(actual[i]),
                    "predicted": float(value),
                }
            )

        model_artifact = None
        if bool(opts.get("includeArtifact")):
            try:
                model_artifact = create_model_artifact(
                    model_artifact_obj if model_artifact_obj is not None else {"kind": model_kind, "horizon": horizon},
                    task,
                    model_kind,
                    target,
                    [],
                    preset,
                )
            except Exception as e:
                warnings.append(f"artifact export unavailable: {str(e)}")

        metrics = {
            "rows": int(len(ts_df)),
            "horizon": int(len(actual)),
            "mae": float(sum(abs_err) / max(1, len(abs_err))),
            "rmse": float(math.sqrt(sum(sq_err) / max(1, len(sq_err)))),
        }
        if mape_terms:
            metrics["mape"] = float(sum(mape_terms) / len(mape_terms))

        return {
            "ok": True,
            "data": {
                "task": task,
                "model": model_kind,
                "preset": preset,
                "target": target,
                "timeColumn": time_col,
                "features": [],
                "encodedFeatureCount": 0,
                "metrics": metrics,
                "errorAnalysis": {
                    "type": "timeseries",
                    "residualSummary": {
                        "count": int(len(residuals)),
                        "mean": float(sum(residuals) / max(1, len(residuals))),
                        "absMean": float(sum(abs(v) for v in residuals) / max(1, len(residuals))),
                        "q05": float(sorted_res[q05_idx]),
                        "q95": float(sorted_res[q95_idx]),
                    },
                },
                "timeSeriesPreview": {
                    "historyTail": history_tail,
                    "forecast": forecast_rows,
                },
                "modelArtifact": model_artifact,
                "warnings": warnings,
            },
        }

    if not features:
        if supervised:
            features = [c for c in df.columns if c != target]
        else:
            features = [c for c in df.columns]
    if not isinstance(features, list) or not features:
        return {"ok": False, "code": "ML_FEATURES_REQUIRED", "message": "at least one feature is required"}

    missing = [c for c in features if c not in df.columns]
    if missing:
        return {
            "ok": False,
            "code": "ML_FEATURES_NOT_FOUND",
            "message": "some features were not found",
            "details": missing,
        }

    select_cols = list(features)
    if supervised:
        select_cols = features + [target]
    data = df[select_cols].copy()
    if supervised:
        data = data.dropna(subset=[target])
    else:
        data = data.dropna(subset=features)
    if data.empty:
        return {
            "ok": False,
            "code": "ML_EMPTY_AFTER_CLEAN",
            "message": "no rows left after cleaning",
        }

    X_raw = data[features]
    X = pd.get_dummies(X_raw, drop_first=True)
    if X.shape[1] == 0:
        return {"ok": False, "code": "ML_EMPTY_FEATURE_MATRIX", "message": "feature matrix is empty"}

    warnings = []
    if invalid_preset:
        warnings.append("unsupported preset; fallback to balanced")
    ensemble_estimators_map = {"fast": 120, "balanced": 200, "accurate": 400}
    extra_estimators_map = {"fast": 160, "balanced": 300, "accurate": 500}
    boost_estimators_map = {"fast": 80, "balanced": 140, "accurate": 220}
    hgb_iter_map = {"fast": 100, "balanced": 200, "accurate": 350}
    nn_hidden_map = {
        "fast": (32,),
        "balanced": (64, 32),
        "accurate": (128, 64, 32),
    }
    nn_iter_map = {"fast": 280, "balanced": 600, "accurate": 1000}
    elastic_iter_map = {"fast": 1200, "balanced": 2000, "accurate": 4000}
    kmeans_n_init_map = {"fast": 5, "balanced": 10, "accurate": 20}
    ensemble_estimators = ensemble_estimators_map[preset]
    extra_estimators = extra_estimators_map[preset]
    boost_estimators = boost_estimators_map[preset]
    hgb_iters = hgb_iter_map[preset]
    nn_hidden = nn_hidden_map[preset]
    nn_iters = nn_iter_map[preset]
    elastic_iters = elastic_iter_map[preset]
    kmeans_n_init = kmeans_n_init_map[preset]
    cv_folds = as_int(opts.get("cv", 0), 0)
    scoring_raw = str(opts.get("scoring") or "").strip().lower()
    include_artifact = bool(opts.get("includeArtifact"))
    enable_shap = bool(opts.get("enableShap"))

    if task == "regression":
        y_raw = data[target]
        y = y_raw.map(as_float)
        mask = y.map(lambda v: not math.isnan(v))
        X = X[mask]
        y = y[mask]
        if len(y) < 8:
            return {
                "ok": False,
                "code": "ML_TOO_FEW_ROWS",
                "message": "regression needs at least 8 valid rows",
            }
    elif task == "classification":
        y_raw = data[target]
        y = y_raw.astype(str)
        if y.nunique() < 2:
            return {
                "ok": False,
                "code": "ML_CLASS_LABELS_REQUIRED",
                "message": "classification needs at least two classes",
            }
        if y.nunique() > 50:
            warnings.append("classification target has many classes; metrics may be unstable")

    random_state = as_int(opts.get("randomState", 42), 42)
    model_alias = {
        "rf": "forest",
        "random_forest": "forest",
        "randomforest": "forest",
        "logistic": "linear",
        "mlp": "nn",
        "neural": "nn",
        "elastic_net": "elasticnet",
        "histgb": "hgb",
        "hist_gradient_boosting": "hgb",
        "naive_bayes": "nb",
        "gaussiannb": "nb",
        "k_neighbor": "knn",
        "k_neighbors": "knn",
        "isolationforest": "isolation_forest",
        "iforest": "isolation_forest",
        "extratrees": "extra_trees",
        "extra_trees": "extra_trees",
        "ada_boost": "adaboost",
        "svc": "svm",
        "svr": "svm",
        "calibration": "calibrated",
        "ensemble": "voting",
        "moving_average": "moving_avg",
        "exp": "exp_smoothing",
        "holtwinters": "exp_smoothing",
        "auto_encoder": "autoencoder",
        "torch": "torch_mlp",
        "pytorch_mlp": "torch_mlp",
        "tf": "tf_mlp",
        "tensorflow_mlp": "tf_mlp",
        "fttransformer": "ft_transformer",
        "ft-transformer": "ft_transformer",
        "xgb": "xgboost",
        "lgbm": "lightgbm",
        "lgb": "lightgbm",
        "cat": "catboost",
    }
    model_kind = model_alias.get(model_kind, model_kind)

    if task == "anomaly":
        if cv_folds >= 2 or scoring_raw:
            warnings.append("cv/scoring options are ignored for unsupervised tasks")
        if model_kind not in {"isolation_forest", "autoencoder"}:
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}

        contamination = opts.get("contamination", "auto")
        if contamination != "auto":
            contamination = as_ratio(contamination, -1.0)
            if contamination <= 0:
                contamination = "auto"
                warnings.append("invalid contamination value; fallback to auto")
        feature_importance = []
        score_stats = {}
        if model_kind == "isolation_forest":
            model = IsolationForest(
                contamination=contamination,
                random_state=random_state,
                n_estimators=ensemble_estimators,
            )
            model.fit(X)

            labels_raw = model.predict(X)
            labels = [int(v) for v in labels_raw.tolist()]
            feature_importance = feature_importance_map(model, list(X.columns))

            if hasattr(model, "decision_function"):
                try:
                    scores = [float(v) for v in model.decision_function(X).tolist()]
                    if scores:
                        score_stats = {
                            "score_min": float(min(scores)),
                            "score_max": float(max(scores)),
                            "score_mean": float(sum(scores) / len(scores)),
                        }
                except Exception:
                    pass
        else:
            ae_hidden = as_int(opts.get("aeHidden", max(4, min(64, int(X.shape[1]) * 2))), max(4, min(64, int(X.shape[1]) * 2)))
            if ae_hidden < 2:
                ae_hidden = 2
            model = MLPRegressor(
                hidden_layer_sizes=(ae_hidden,),
                random_state=random_state,
                max_iter=nn_iters,
            )
            model.fit(X, X)
            recon = model.predict(X)
            errors = []
            for idx in range(int(X.shape[0])):
                src = [float(v) for v in X.iloc[idx].tolist()]
                rec = [float(v) for v in recon[idx]]
                if not src:
                    errors.append(0.0)
                    continue
                mse_val = sum((a - b) ** 2 for a, b in zip(src, rec)) / len(src)
                errors.append(float(mse_val))

            contamination_ratio = 0.05 if contamination == "auto" else float(contamination)
            if contamination_ratio <= 0 or contamination_ratio >= 0.5:
                contamination_ratio = 0.05
            sorted_err = sorted(errors)
            cut_idx = int(max(0, min(len(sorted_err) - 1, math.floor((1.0 - contamination_ratio) * (len(sorted_err) - 1)))))
            threshold = float(sorted_err[cut_idx])
            labels = [-1 if v >= threshold else 1 for v in errors]
            feature_importance = autoencoder_importance_map(model, list(X.columns))
            score_stats = {
                "recon_error_min": float(min(errors)) if errors else 0.0,
                "recon_error_max": float(max(errors)) if errors else 0.0,
                "recon_error_mean": float(sum(errors) / len(errors)) if errors else 0.0,
                "recon_error_threshold": threshold,
            }

        outlier_count = sum(1 for v in labels if v == -1)
        inlier_count = len(labels) - outlier_count
        model_artifact = None
        if include_artifact:
            try:
                model_artifact = create_model_artifact(
                    model,
                    task,
                    model_kind,
                    target,
                    list(X.columns),
                    preset,
                )
            except Exception as e:
                warnings.append(f"artifact export unavailable: {str(e)}")

        return {
            "ok": True,
            "data": {
                "task": task,
                "model": model_kind,
                "preset": preset,
                "features": features,
                "encodedFeatureCount": int(X.shape[1]),
                "metrics": {
                    "rows": int(len(labels)),
                    "outlier_count": int(outlier_count),
                    "inlier_count": int(inlier_count),
                    "outlier_ratio": float(outlier_count / len(labels)) if labels else 0.0,
                    **score_stats,
                },
                "featureImportance": feature_importance,
                "anomalySummary": {
                    "labelCounts": [
                        {"label": "-1", "count": int(outlier_count)},
                        {"label": "1", "count": int(inlier_count)},
                    ],
                },
                "modelArtifact": model_artifact,
                "warnings": warnings,
            },
        }

    if task == "clustering":
        if cv_folds >= 2 or scoring_raw:
            warnings.append("cv/scoring options are ignored for unsupervised tasks")
        if model_kind == "kmeans":
            n_clusters = as_int(opts.get("nClusters", 3), 3)
            if n_clusters < 2:
                n_clusters = 2
            if n_clusters >= len(X):
                n_clusters = max(2, len(X) - 1)
                warnings.append("nClusters was too large for row count; adjusted automatically")
            if n_clusters < 2:
                return {
                    "ok": False,
                    "code": "ML_TOO_FEW_ROWS",
                    "message": "clustering needs at least 3 valid rows",
                }
            model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=kmeans_n_init)
        elif model_kind == "dbscan":
            eps = float(opts.get("eps", 0.5))
            min_samples = as_int(opts.get("minSamples", 5), 5)
            if min_samples < 1:
                min_samples = 1
            model = DBSCAN(eps=eps, min_samples=min_samples)
        else:
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}

        labels_raw = model.fit_predict(X)
        labels = [int(v) for v in labels_raw.tolist()]

        counts = {}
        for lb in labels:
            key = str(lb)
            counts[key] = counts.get(key, 0) + 1
        cluster_summary = [{"cluster": k, "count": int(v)} for k, v in sorted(counts.items(), key=lambda x: int(x[0]))]
        cluster_count = len([k for k in counts.keys() if k != "-1"])
        noise_count = int(counts.get("-1", 0))

        silhouette = None
        try:
            if model_kind == "dbscan":
                keep_idx = [i for i, lb in enumerate(labels) if lb != -1]
                if len(keep_idx) >= 3:
                    kept_labels = [labels[i] for i in keep_idx]
                    if len(set(kept_labels)) >= 2:
                        silhouette = float(silhouette_score(X.iloc[keep_idx], kept_labels))
            else:
                if len(set(labels)) >= 2:
                    silhouette = float(silhouette_score(X, labels))
        except Exception:
            silhouette = None

        if cluster_count < 2:
            warnings.append("less than 2 clusters detected; review features or model settings")
        model_artifact = None
        if include_artifact:
            try:
                model_artifact = create_model_artifact(
                    model,
                    task,
                    model_kind,
                    target,
                    list(X.columns),
                    preset,
                )
            except Exception as e:
                warnings.append(f"artifact export unavailable: {str(e)}")

        metrics = {
            "rows": int(len(labels)),
            "cluster_count": int(cluster_count),
            "noise_count": int(noise_count),
        }
        if silhouette is not None:
            metrics["silhouette"] = float(silhouette)

        return {
            "ok": True,
            "data": {
                "task": task,
                "model": model_kind,
                "preset": preset,
                "features": features,
                "encodedFeatureCount": int(X.shape[1]),
                "metrics": metrics,
                "clusterSummary": cluster_summary,
                "modelArtifact": model_artifact,
                "warnings": warnings,
            },
        }

    if task == "dim_reduction":
        if cv_folds >= 2 or scoring_raw:
            warnings.append("cv/scoring options are ignored for unsupervised tasks")
        if model_kind != "pca":
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}

        max_components = min(int(X.shape[0]), int(X.shape[1]))
        if max_components < 1:
            return {
                "ok": False,
                "code": "ML_EMPTY_FEATURE_MATRIX",
                "message": "feature matrix is empty",
            }

        n_components = as_int(opts.get("nComponents", 2), 2)
        if n_components < 1:
            n_components = 1
        if n_components > max_components:
            n_components = max_components
            warnings.append("nComponents was too large and was adjusted automatically")

        model = PCA(n_components=n_components)
        transformed = model.fit_transform(X)

        explained = [float(v) for v in model.explained_variance_ratio_.tolist()]
        preview = []
        max_preview = min(int(transformed.shape[0]), 20)
        for i in range(max_preview):
            row = {"rowIndex": int(i)}
            for j in range(n_components):
                row[f"pc{j + 1}"] = float(transformed[i][j])
            preview.append(row)

        component_loadings = []
        try:
            comp_matrix = model.components_.tolist()
            encoded_cols = list(X.columns)
            for idx, comp in enumerate(comp_matrix):
                pairs = []
                for name, loading in zip(encoded_cols, comp):
                    pairs.append({"feature": name, "loading": float(loading)})
                pairs.sort(key=lambda x: abs(x["loading"]), reverse=True)
                component_loadings.append({"component": int(idx + 1), "topLoadings": pairs[:10]})
        except Exception:
            component_loadings = []
        model_artifact = None
        if include_artifact:
            try:
                model_artifact = create_model_artifact(
                    model,
                    task,
                    model_kind,
                    target,
                    list(X.columns),
                    preset,
                )
            except Exception as e:
                warnings.append(f"artifact export unavailable: {str(e)}")

        return {
            "ok": True,
            "data": {
                "task": task,
                "model": model_kind,
                "preset": preset,
                "features": features,
                "encodedFeatureCount": int(X.shape[1]),
                "metrics": {
                    "rows": int(X.shape[0]),
                    "components": int(n_components),
                    "explained_variance_total": float(sum(explained) if explained else 0.0),
                },
                "explainedVarianceRatio": explained,
                "projectionPreview": preview,
                "componentLoadings": component_loadings,
                "modelArtifact": model_artifact,
                "warnings": warnings,
            },
        }

    test_size = as_ratio(opts.get("testSize", 0.2), 0.2)
    if len(X) < 20:
        X_train = X
        X_test = X
        y_train = y
        y_test = y
        warnings.append("dataset is small; metrics are computed on training data")
    else:
        stratify = y if task == "classification" else None
        try:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=float(test_size), random_state=random_state, stratify=stratify
            )
        except Exception:
            X_train = X
            X_test = X
            y_train = y
            y_test = y
            warnings.append("train/test split failed; metrics are computed on training data")

    deep_backend_kind = None
    if task == "regression":
        if model_kind == "linear":
            model = LinearRegression()
        elif model_kind == "tree":
            model = DecisionTreeRegressor(random_state=random_state)
        elif model_kind == "forest":
            model = RandomForestRegressor(n_estimators=ensemble_estimators, random_state=random_state)
        elif model_kind == "extra_trees":
            model = ExtraTreesRegressor(n_estimators=extra_estimators, random_state=random_state)
        elif model_kind == "adaboost":
            model = AdaBoostRegressor(n_estimators=boost_estimators, random_state=random_state)
        elif model_kind == "voting":
            model = VotingRegressor(
                estimators=[
                    ("linear", LinearRegression()),
                    ("forest", RandomForestRegressor(n_estimators=max(80, ensemble_estimators - 40), random_state=random_state)),
                    ("extra", ExtraTreesRegressor(n_estimators=max(80, extra_estimators - 60), random_state=random_state)),
                ]
            )
        elif model_kind == "elasticnet":
            alpha = float(opts.get("alpha", 1.0))
            l1_ratio = float(opts.get("l1Ratio", 0.5))
            model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, max_iter=elastic_iters)
        elif model_kind == "svm":
            svm_c = float(opts.get("svmC", 1.0))
            svm_epsilon = float(opts.get("svmEpsilon", 0.1))
            svm_kernel = str(opts.get("svmKernel", "linear" if preset == "fast" else "rbf") or ("linear" if preset == "fast" else "rbf"))
            model = SVR(C=svm_c, epsilon=svm_epsilon, kernel=svm_kernel)
        elif model_kind == "hgb":
            model = HistGradientBoostingRegressor(random_state=random_state, max_iter=hgb_iters)
        elif model_kind == "nn":
            model = MLPRegressor(hidden_layer_sizes=nn_hidden, random_state=random_state, max_iter=nn_iters)
        elif model_kind == "xgboost":
            if XGBRegressor is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "xgboost is not installed",
                    "details": {"model": model_kind, "dependency": "xgboost"},
                }
            model = XGBRegressor(
                n_estimators=ensemble_estimators,
                random_state=random_state,
                objective="reg:squarederror",
                verbosity=0,
            )
        elif model_kind == "lightgbm":
            if LGBMRegressor is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "lightgbm is not installed",
                    "details": {"model": model_kind, "dependency": "lightgbm"},
                }
            model = LGBMRegressor(
                n_estimators=ensemble_estimators,
                random_state=random_state,
                objective="regression",
                verbosity=-1,
            )
        elif model_kind == "catboost":
            if CatBoostRegressor is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "catboost is not installed",
                    "details": {"model": model_kind, "dependency": "catboost"},
                }
            model = CatBoostRegressor(
                iterations=ensemble_estimators,
                random_seed=random_state,
                verbose=False,
            )
        elif model_kind in {"tabnet", "torch_mlp", "tf_mlp", "ft_transformer"}:
            deep_backend_kind = model_kind
            model = None
        else:
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}
    else:
        if model_kind == "linear":
            model = LogisticRegression(max_iter=1000)
        elif model_kind == "tree":
            model = DecisionTreeClassifier(random_state=random_state)
        elif model_kind == "forest":
            model = RandomForestClassifier(n_estimators=ensemble_estimators, random_state=random_state)
        elif model_kind == "extra_trees":
            model = ExtraTreesClassifier(n_estimators=extra_estimators, random_state=random_state)
        elif model_kind == "adaboost":
            model = AdaBoostClassifier(n_estimators=boost_estimators, random_state=random_state)
        elif model_kind == "voting":
            model = VotingClassifier(
                estimators=[
                    ("logit", LogisticRegression(max_iter=1000)),
                    ("forest", RandomForestClassifier(n_estimators=max(80, ensemble_estimators - 40), random_state=random_state)),
                    ("extra", ExtraTreesClassifier(n_estimators=max(80, extra_estimators - 60), random_state=random_state)),
                ],
                voting="soft",
            )
        elif model_kind == "calibrated":
            calib_method = str(opts.get("calibMethod", "sigmoid") or "sigmoid")
            calib_cv = as_int(opts.get("calibCv", 3), 3)
            if calib_cv < 2:
                calib_cv = 2

            class_counts = y_train.value_counts() if hasattr(y_train, "value_counts") else None
            max_cv = int(class_counts.min()) if class_counts is not None and len(class_counts) else 2
            if max_cv < 2:
                return {
                    "ok": False,
                    "code": "ML_TOO_FEW_ROWS",
                    "message": "calibrated classifier needs at least 2 samples per class in train split",
                }
            if calib_cv > max_cv:
                calib_cv = max_cv
                warnings.append("calibCv was too large for class counts and was adjusted automatically")

            base_model = RandomForestClassifier(n_estimators=ensemble_estimators, random_state=random_state)
            try:
                model = CalibratedClassifierCV(estimator=base_model, method=calib_method, cv=calib_cv)
            except TypeError:
                model = CalibratedClassifierCV(base_estimator=base_model, method=calib_method, cv=calib_cv)
        elif model_kind == "hgb":
            model = HistGradientBoostingClassifier(random_state=random_state, max_iter=hgb_iters)
        elif model_kind == "nb":
            model = GaussianNB()
        elif model_kind == "knn":
            n_neighbors = as_int(opts.get("nNeighbors", 5), 5)
            if n_neighbors < 1:
                n_neighbors = 1
            model = KNeighborsClassifier(n_neighbors=n_neighbors)
        elif model_kind == "svm":
            svm_c = float(opts.get("svmC", 1.0))
            svm_kernel = str(opts.get("svmKernel", "linear" if preset == "fast" else "rbf") or ("linear" if preset == "fast" else "rbf"))
            svm_gamma = opts.get("svmGamma", "scale")
            model = SVC(C=svm_c, kernel=svm_kernel, gamma=svm_gamma)
        elif model_kind == "nn":
            model = MLPClassifier(hidden_layer_sizes=nn_hidden, random_state=random_state, max_iter=nn_iters)
        elif model_kind == "xgboost":
            if XGBClassifier is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "xgboost is not installed",
                    "details": {"model": model_kind, "dependency": "xgboost"},
                }
            model = XGBClassifier(
                n_estimators=ensemble_estimators,
                random_state=random_state,
                eval_metric="logloss",
                verbosity=0,
            )
        elif model_kind == "lightgbm":
            if LGBMClassifier is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "lightgbm is not installed",
                    "details": {"model": model_kind, "dependency": "lightgbm"},
                }
            model = LGBMClassifier(
                n_estimators=ensemble_estimators,
                random_state=random_state,
                objective="multiclass",
                verbosity=-1,
            )
        elif model_kind == "catboost":
            if CatBoostClassifier is None:
                return {
                    "ok": False,
                    "code": "ML_DEP_MISSING",
                    "message": "catboost is not installed",
                    "details": {"model": model_kind, "dependency": "catboost"},
                }
            model = CatBoostClassifier(
                iterations=ensemble_estimators,
                random_seed=random_state,
                verbose=False,
            )
        elif model_kind in {"tabnet", "torch_mlp", "tf_mlp", "ft_transformer"}:
            deep_backend_kind = model_kind
            model = None
        else:
            return {"ok": False, "code": "ML_MODEL_INVALID", "message": f"unsupported model: {model_kind}"}

    scoring_alias_map = {
        "regression": {
            "r2": "r2",
            "mae": "neg_mean_absolute_error",
            "neg_mae": "neg_mean_absolute_error",
            "neg_mean_absolute_error": "neg_mean_absolute_error",
            "mse": "neg_mean_squared_error",
            "neg_mse": "neg_mean_squared_error",
            "neg_mean_squared_error": "neg_mean_squared_error",
            "rmse": "neg_root_mean_squared_error",
            "neg_rmse": "neg_root_mean_squared_error",
            "neg_root_mean_squared_error": "neg_root_mean_squared_error",
        },
        "classification": {
            "accuracy": "accuracy",
            "f1": "f1_weighted",
            "f1_weighted": "f1_weighted",
            "precision": "precision_weighted",
            "precision_weighted": "precision_weighted",
            "recall": "recall_weighted",
            "recall_weighted": "recall_weighted",
        },
    }
    scoring_default = "r2" if task == "regression" else "accuracy"
    scoring_alias = scoring_alias_map.get(task, {})
    scoring_used = scoring_alias.get(scoring_raw, scoring_raw if scoring_raw else scoring_default)
    if scoring_used not in set(scoring_alias.values()):
        return {
            "ok": False,
            "code": "ML_SCORING_INVALID",
            "message": f"unsupported scoring: {scoring_raw}",
            "details": sorted(set(scoring_alias.values())),
        }

    cv_summary = {"enabled": False}
    if deep_backend_kind is not None and cv_folds >= 2:
        warnings.append("cv is currently disabled for deep backend models")
    if cv_folds >= 2 and deep_backend_kind is None:
        if task == "classification":
            class_counts = y.value_counts() if hasattr(y, "value_counts") else None
            max_cv = int(class_counts.min()) if class_counts is not None and len(class_counts) else 0
            if max_cv < 2:
                return {
                    "ok": False,
                    "code": "ML_TOO_FEW_ROWS",
                    "message": "classification cv needs at least 2 samples per class",
                }
            if cv_folds > max_cv:
                cv_folds = max_cv
                warnings.append("cv folds were adjusted to fit smallest class count")
            cv_strategy = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=random_state)
        else:
            if cv_folds > len(X):
                cv_folds = len(X)
                warnings.append("cv folds were adjusted to fit row count")
            if cv_folds < 2:
                cv_folds = 0
            cv_strategy = KFold(n_splits=cv_folds, shuffle=True, random_state=random_state) if cv_folds >= 2 else None

        if cv_folds >= 2 and cv_strategy is not None:
            try:
                cv_scores_raw = cross_val_score(model, X, y, cv=cv_strategy, scoring=scoring_used)
                cv_scores = [float(v) for v in cv_scores_raw.tolist()]
                cv_summary = {
                    "enabled": True,
                    "folds": int(cv_folds),
                    "scoring": scoring_used,
                    "mean": float(sum(cv_scores) / len(cv_scores)) if cv_scores else None,
                    "std": float((sum((s - (sum(cv_scores) / len(cv_scores))) ** 2 for s in cv_scores) / len(cv_scores)) ** 0.5)
                    if cv_scores
                    else None,
                    "min": float(min(cv_scores)) if cv_scores else None,
                    "max": float(max(cv_scores)) if cv_scores else None,
                    "scores": cv_scores[:20],
                }
            except Exception as e:
                warnings.append(f"cv failed and was omitted: {str(e)}")

    deep_feature_importance = []
    model_for_artifact = model
    if deep_backend_kind is not None:
        deep_out = train_deep_backend(
            deep_backend_kind,
            task,
            X_train,
            X_test,
            y_train,
            y_test,
            random_state,
            preset,
        )
        if not deep_out.get("ok", False):
            return deep_out
        pred = deep_out.get("pred", [])
        deep_feature_importance = deep_out.get("featureImportance", []) or []
        model_for_artifact = deep_out.get("artifactObj")
        warnings.extend(deep_out.get("warnings", []) or [])
    else:
        model.fit(X_train, y_train)
        pred = model.predict(X_test)

    error_analysis = {}
    if task == "regression":
        mse = float(mean_squared_error(y_test, pred))
        metrics = {
            "r2": float(r2_score(y_test, pred)),
            "mae": float(mean_absolute_error(y_test, pred)),
            "rmse": float(math.sqrt(mse)),
        }
        try:
            y_test_list = list(y_test)
            pred_list = list(pred)
            residuals = [float(a - b) for a, b in zip(y_test_list, pred_list)]
            if residuals:
                sorted_res = sorted(residuals)
                q05_idx = max(0, min(len(sorted_res) - 1, int(round((len(sorted_res) - 1) * 0.05))))
                q95_idx = max(0, min(len(sorted_res) - 1, int(round((len(sorted_res) - 1) * 0.95))))
                abs_sorted_idx = sorted(range(len(residuals)), key=lambda i: abs(residuals[i]), reverse=True)
                top_items = []
                for i in abs_sorted_idx[:10]:
                    top_items.append(
                        {
                            "actual": float(y_test_list[i]),
                            "predicted": float(pred_list[i]),
                            "residual": float(residuals[i]),
                        }
                    )
                error_analysis = {
                    "type": "regression",
                    "residualSummary": {
                        "count": int(len(residuals)),
                        "mean": float(sum(residuals) / len(residuals)),
                        "absMean": float(sum(abs(v) for v in residuals) / len(residuals)),
                        "min": float(sorted_res[0]),
                        "q05": float(sorted_res[q05_idx]),
                        "median": float(sorted_res[len(sorted_res) // 2]),
                        "q95": float(sorted_res[q95_idx]),
                        "max": float(sorted_res[-1]),
                    },
                    "topResiduals": top_items,
                }
        except Exception:
            error_analysis = {}
    else:
        metrics = {
            "accuracy": float(accuracy_score(y_test, pred)),
            "precision_weighted": float(precision_score(y_test, pred, average="weighted", zero_division=0)),
            "recall_weighted": float(recall_score(y_test, pred, average="weighted", zero_division=0)),
            "f1_weighted": float(f1_score(y_test, pred, average="weighted", zero_division=0)),
        }
        try:
            y_test_list = [str(v) for v in list(y_test)]
            pred_list = [str(v) for v in list(pred)]
            labels = sorted(set(y_test_list) | set(pred_list))
            matrix = confusion_matrix(y_test_list, pred_list, labels=labels).tolist()
            error_analysis = {"type": "classification", "labels": labels, "matrix": matrix}
        except Exception:
            error_analysis = {}

    if deep_backend_kind is not None:
        importance = deep_feature_importance
    else:
        importance = feature_importance_map(model, list(X.columns))
    permutation_importance_values = []
    if deep_backend_kind is not None:
        warnings.append("permutation importance is disabled for deep backend models")
    else:
        try:
            perm_X = X_test
            perm_y = y_test
            if hasattr(X_test, "shape") and int(X_test.shape[0]) > 1200 and hasattr(X_test, "sample"):
                perm_X = X_test.sample(n=1200, random_state=random_state)
                if hasattr(y_test, "loc"):
                    perm_y = y_test.loc[perm_X.index]
                warnings.append("permutation importance used sampled holdout rows (1200)")
            permutation_importance_values = permutation_importance_map(
                model,
                perm_X,
                perm_y,
                list(X.columns),
                scoring_used,
                random_state,
            )
        except Exception as e:
            warnings.append(f"permutation importance unavailable: {str(e)}")
    shap_importance_values = []
    if enable_shap and deep_backend_kind is not None:
        warnings.append("shap is disabled for deep backend models")
    if enable_shap and deep_backend_kind is None:
        try:
            shap_source = X_train if hasattr(X_train, "shape") and int(X_train.shape[0]) >= 2 else X
            shap_importance_values = shap_importance_map(model, shap_source, list(X.columns))
            if not shap_importance_values:
                warnings.append("shap returned empty importance")
        except Exception as e:
            warnings.append(f"shap unavailable: {str(e)}")
    model_artifact = None
    if include_artifact:
        try:
            model_artifact = create_model_artifact(
                model_for_artifact,
                task,
                model_kind,
                target,
                list(X.columns),
                preset,
            )
        except Exception as e:
            warnings.append(f"artifact export unavailable: {str(e)}")
    return {
        "ok": True,
        "data": {
            "task": task,
            "model": model_kind,
            "preset": preset,
            "target": target,
            "features": features,
            "encodedFeatureCount": int(X.shape[1]),
            "metrics": metrics,
            "featureImportance": importance,
            "permutationImportance": permutation_importance_values,
            "shapImportance": shap_importance_values,
            "errorAnalysis": error_analysis,
            "modelArtifact": model_artifact,
            "validation": {
                "holdoutTestSize": float(test_size),
                "scoring": scoring_used,
                "cv": cv_summary,
            },
            "warnings": warnings,
        },
    }


def main() -> None:
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw else {}
        op = str(payload.get("op") or "train").strip().lower()
        if op == "capabilities":
            emit(check_capabilities())
            return
        if op != "train":
            fail("ML_OP_INVALID", f"unsupported op: {op}")
            return
        emit(normalize_success_contract(train(payload)))
    except Exception as e:
        fail("ML_UNHANDLED_EXCEPTION", "unhandled ml exception", str(e))


if __name__ == "__main__":
    main()
