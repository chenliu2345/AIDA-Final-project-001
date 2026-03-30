"""
Model 1 — Random Forest: Optimal Price Prediction (Baseline)
Data  : V_Y1 WHERE Status_Label = 'SOLD'
Tuning: Fixed hyperparameters (no Optuna)
Target: log(Price_CAD) — reversed to CAD for evaluation

Distance features:
  Distance_from_Edmonton_KM and Distance_from_Calgary_KM are read from
  V_Y1 at training time (pre-stored in tbl_Locations by the ETL via OSRM).
  At inference time, these values are fetched in real time from the OSRM
  API inside inference/predict.py given the seller's city name.
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sqlalchemy import create_engine, text

from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DB_SERVER  = "."
DB_NAME    = "AB_CarSale_DB"
DB_DRIVER  = "ODBC+Driver+17+for+SQL+Server"

MODEL_PATH = Path(__file__).parent.parent / "models" / "rf_price_model.pkl"

CV_FOLDS    = 5
RANDOM_SEED = 42

# Fixed hyperparameters — based on Optuna search results, max_features=0.3
# is the most impactful setting when features are OHE-expanded.
RF_PARAMS = {
    "n_estimators":      200,
    "max_depth":         20,
    "min_samples_split": 10,
    "min_samples_leaf":  1,
    "max_features":      0.3,
}

# Distance columns are numeric — stored in DB at ETL time via OSRM API,
# fetched in real time from OSRM at inference time.
NUM_FEATURES = [
    "Year", "Kilometres",
    "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM",
]
CAT_FEATURES = [
    "Condition_Label", "Transmission_Type", "Drivetrain_Type",
    "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim",
]
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET       = "Price_CAD"


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 1 — LOAD
#  Query V_Y1 filtered to SOLD status only
# ─────────────────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    engine = create_engine(
        f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}"
        f"?driver={DB_DRIVER}&trusted_connection=yes"
    )
    query = text("""
        SELECT *
        FROM dbo.V_Y1
        WHERE Status_Label = 'SOLD'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    engine.dispose()
    print(f"Loaded {len(df):,} SOLD rows from V_Y1")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2 — PREPARE
# ─────────────────────────────────────────────────────────────────────────────
def prepare(df: pd.DataFrame):
    df = df[ALL_FEATURES + [TARGET]].copy()
    df = df.dropna(subset=[TARGET, "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM"])
    X = df[ALL_FEATURES]
    y = np.log1p(df[TARGET].astype(float))  # log transform — price is right-skewed
    return X, y


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — BUILD PIPELINE
#
#  Unlike CatBoost, Random Forest cannot handle raw strings.
#  The Pipeline handles this automatically:
#    Numeric    → median imputation
#    Categorical → fill NaN + OneHotEncoder (handle_unknown='ignore')
#
#  handle_unknown='ignore' ensures unseen categories at predict-time
#  are encoded as all-zeros rather than raising an error.
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline() -> Pipeline:
    numeric_transformer = SimpleImputer(strategy="median")

    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer,      NUM_FEATURES),
            ("cat", categorical_transformer,  CAT_FEATURES),
        ]
    )

    model = RandomForestRegressor(
        **RF_PARAMS,
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )

    return Pipeline([
        ("preprocessor", preprocessor),
        ("model",        model),
    ])


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4 — CROSS-VALIDATE
# ─────────────────────────────────────────────────────────────────────────────
def evaluate(X: pd.DataFrame, y: pd.Series) -> None:
    cv      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    metrics = {"R2": [], "MAE": [], "RMSE": []}

    print(f"\n{'─'*55}")
    print(f"Random Forest — {CV_FOLDS}-Fold Cross-Validation")
    print(f"{'─'*55}")

    for fold, (train_idx, val_idx) in enumerate(cv.split(X), 1):
        X_tr,  X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr,  y_val = y.iloc[train_idx], y.iloc[val_idx]

        pipeline = build_pipeline()
        pipeline.fit(X_tr, y_tr)

        # Reverse log transform before computing metrics so results are in CAD
        y_pred_actual = np.expm1(pipeline.predict(X_val))
        y_val_actual  = np.expm1(y_val)

        metrics["R2"].append(r2_score(y_val_actual, y_pred_actual))
        metrics["MAE"].append(mean_absolute_error(y_val_actual, y_pred_actual))
        metrics["RMSE"].append(root_mean_squared_error(y_val_actual, y_pred_actual))

        print(f"  Fold {fold}:  R²={metrics['R2'][-1]:.4f}  "
              f"MAE={metrics['MAE'][-1]:.0f}  "
              f"RMSE={metrics['RMSE'][-1]:.0f}")

    print(f"{'─'*55}")
    for name, vals in metrics.items():
        arr = np.array(vals)
        print(f"  CV {name:4s}: {arr.mean():.4f} ± {arr.std():.4f}")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 5 — FINAL FIT + FEATURE IMPORTANCE + SAVE
#  Feature importance is aggregated back to original feature names
#  after OHE expansion for readability.
# ─────────────────────────────────────────────────────────────────────────────
def fit_final(X: pd.DataFrame, y: pd.Series) -> None:
    print(f"\nFitting final pipeline on full dataset ...")
    pipeline = build_pipeline()
    pipeline.fit(X, y)

    # Recover original feature names after OHE expansion
    ohe        = pipeline.named_steps["preprocessor"].named_transformers_["cat"]["encoder"]
    ohe_names  = ohe.get_feature_names_out(CAT_FEATURES).tolist()
    all_names  = NUM_FEATURES + ohe_names

    raw_imp = pipeline.named_steps["model"].feature_importances_

    # Aggregate OHE-expanded importances back to original category columns
    imp_series = pd.Series(raw_imp, index=all_names)
    agg = {}
    for feat in NUM_FEATURES:
        agg[feat] = imp_series[feat]
    for feat in CAT_FEATURES:
        cols = [c for c in all_names if c.startswith(f"{feat}_")]
        agg[feat] = imp_series[cols].sum()

    imp_agg = pd.Series(agg).sort_values(ascending=False)
    print("\n  Feature Importances (aggregated):")
    print(imp_agg.to_string())

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"\n  Pipeline saved → {MODEL_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df   = load_data()
    X, y = prepare(df)
    evaluate(X, y)
    fit_final(X, y)