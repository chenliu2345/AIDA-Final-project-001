"""
Model 1: Random Forest — Price Prediction with Cross-Validation
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
import joblib

# ── Config ────────────────────────────────────────────────────────────────────
DB_SERVER   = "."
DB_NAME     = "AB_CarSale_DB"
DB_DRIVER   = "ODBC+Driver+17+for+SQL+Server"
VIEW_NAME   = "dbo.V_CarSales_ML"
from pathlib import Path
MODEL_PATH  = Path(__file__).parent / "rf_price_model.pkl"
CV_FOLDS    = 5
RANDOM_SEED = 42

NUM_FEATURES = ["Kilometres", "Vehicle_Age"]
CAT_FEATURES = ["Condition", "Transmission", "Drivetrain", "Body_Style",
                "Colour", "Seats", "City", "Base_Model", "Trim"]
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
    df = df.dropna(subset=[TARGET] + NUM_FEATURES + CAT_FEATURES)
    X = df[NUM_FEATURES + CAT_FEATURES]
    y = df[TARGET].astype(float)
    return X, y


# ── 3. Build pipeline ─────────────────────────────────────────────────────────
def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(transformers=[
        ("num", "passthrough",  NUM_FEATURES),
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                               CAT_FEATURES),
    ])
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=None,
        min_samples_leaf=3,
        n_jobs=-1,
        random_state=RANDOM_SEED,
    )
    return Pipeline([("prep", preprocessor), ("model", model)])


# ── 4. Cross-validate ─────────────────────────────────────────────────────────
def cross_validate(pipeline: Pipeline, X: pd.DataFrame, y: pd.Series) -> None:
    cv = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)

    for metric, scorer in [
        ("R²",   "r2"),
        ("MAE",  "neg_mean_absolute_error"),
        ("RMSE", "neg_root_mean_squared_error"),
    ]:
        scores = cross_val_score(pipeline, X, y, cv=cv, scoring=scorer, n_jobs=-1)
        if scorer.startswith("neg_"):
            scores = -scores
        print(f"  CV {metric}: {scores.mean():.4f} ± {scores.std():.4f}")


# ── 5. Final fit + feature importance ─────────────────────────────────────────
def fit_and_report(pipeline: Pipeline, X: pd.DataFrame, y: pd.Series) -> None:
    pipeline.fit(X, y)

    # Hold-out evaluation on training set (all data used — CV already gave generalisation)
    y_pred = pipeline.predict(X)
    print(f"\n  Train R²  : {r2_score(y, y_pred):.4f}")
    print(f"  Train MAE : {mean_absolute_error(y, y_pred):.2f}")
    print(f"  Train RMSE: {root_mean_squared_error(y, y_pred):.2f}")

    # Feature importance
    rf      = pipeline.named_steps["model"]
    all_features = NUM_FEATURES + CAT_FEATURES
    imp = pd.Series(rf.feature_importances_, index=all_features).sort_values(ascending=False)
    print("\n  Top 10 Feature Importances:")
    print(imp.head(10).to_string())

    joblib.dump(pipeline, MODEL_PATH)
    print(f"\n  Model saved → {MODEL_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df           = load_data()
    X, y         = prepare(df)
    pipeline     = build_pipeline()

    print(f"\n{'─'*50}")
    print(f"Random Forest  |  {CV_FOLDS}-Fold Cross-Validation")
    print(f"{'─'*50}")
    cross_validate(pipeline, X, y)

    print(f"\nFitting final model on full dataset ...")
    fit_and_report(pipeline, X, y)