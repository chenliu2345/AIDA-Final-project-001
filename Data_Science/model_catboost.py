"""
Model 2: CatBoost — Price Prediction with Cross-Validation
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from catboost import CatBoostRegressor, Pool
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

# ── Config ────────────────────────────────────────────────────────────────────
DB_SERVER   = "."
DB_NAME     = "AB_CarSale_DB"
DB_DRIVER   = "ODBC+Driver+17+for+SQL+Server"
VIEW_NAME   = "dbo.V_Y1"
from pathlib import Path
MODEL_PATH  = Path(__file__).parent.parent / "models" / "catboost_price_model.cbm"
CV_FOLDS    = 5
RANDOM_SEED = 42

NUM_FEATURES = ["Year", "Kilometres"]
CAT_FEATURES = ["Condition_Label", "Transmission_Type", "Drivetrain_Type", "Body_Style",
                "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim"]
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET       = "Price_CAD"

# ── 1. Load data from SQL VIEW ─────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    engine = create_engine(
        f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}"
        f"?driver={DB_DRIVER}&trusted_connection=yes"
    )
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {VIEW_NAME}"), conn)
    engine.dispose()
    print(f"Loaded {len(df):,} rows from {VIEW_NAME}")
    return df


# ── 2. Preprocess ──────────────────────────────────────────────────────────────
def prepare(df: pd.DataFrame):
    df = df.dropna(subset=[TARGET] + ALL_FEATURES)
    # CatBoost accepts raw string categoricals — no encoding needed
    for col in CAT_FEATURES:
        df[col] = df[col].astype(str).fillna("UNKNOWN")
    X = df[ALL_FEATURES]
    y = df[TARGET].astype(float)
    return X, y


# ── 3. Build model ────────────────────────────────────────────────────────────
def build_model() -> CatBoostRegressor:
    return CatBoostRegressor(
        iterations=1000,
        learning_rate=0.05,
        depth=8,
        loss_function="RMSE",
        eval_metric="R2",
        cat_features=CAT_FEATURES,
        random_seed=RANDOM_SEED,
        verbose=False,          # silence per-iteration output
    )


# ── 4. Cross-validate ─────────────────────────────────────────────────────────
def cross_validate(X: pd.DataFrame, y: pd.Series) -> None:
    cv      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    metrics = {"R2": [], "MAE": [], "RMSE": []}

    for fold, (train_idx, val_idx) in enumerate(cv.split(X), 1):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = build_model()
        model.fit(
            Pool(X_tr, y_tr, cat_features=CAT_FEATURES),
            eval_set=Pool(X_val, y_val, cat_features=CAT_FEATURES),
            early_stopping_rounds=50,
        )
        y_pred = model.predict(X_val)
        metrics["R2"].append(r2_score(y_val, y_pred))
        metrics["MAE"].append(mean_absolute_error(y_val, y_pred))
        metrics["RMSE"].append(root_mean_squared_error(y_val, y_pred))
        print(f"  Fold {fold}: R²={metrics['R2'][-1]:.4f}  "
              f"MAE={metrics['MAE'][-1]:.2f}  RMSE={metrics['RMSE'][-1]:.2f}")

    for name, vals in metrics.items():
        arr = np.array(vals)
        print(f"  CV {name}: {arr.mean():.4f} ± {arr.std():.4f}")


# ── 5. Final fit + feature importance ─────────────────────────────────────────
def fit_and_report(X: pd.DataFrame, y: pd.Series) -> None:
    model = build_model()
    model.fit(Pool(X, y, cat_features=CAT_FEATURES))

    y_pred = model.predict(X)
    print(f"\n  Train R²  : {r2_score(y, y_pred):.4f}")
    print(f"  Train MAE : {mean_absolute_error(y, y_pred):.2f}")
    print(f"  Train RMSE: {root_mean_squared_error(y, y_pred):.2f}")

    imp = pd.Series(model.get_feature_importance(), index=ALL_FEATURES).sort_values(ascending=False)
    print("\n  Top 10 Feature Importances:")
    print(imp.head(10).to_string())

    model.save_model(MODEL_PATH)
    print(f"\n  Model saved → {MODEL_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df   = load_data()
    X, y = prepare(df)

    print(f"\n{'─'*50}")
    print(f"CatBoost  |  {CV_FOLDS}-Fold Cross-Validation")
    print(f"{'─'*50}")
    cross_validate(X, y)

    print(f"\nFitting final model on full dataset ...")
    fit_and_report(X, y)