"""
Model 2 — CatBoost: Days-to-Sell Prediction   (training only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Data source : V_Y2  (SOLD records with Days_to_Sell)
Target      : Days_to_Sell
Features    : vehicle attributes  +  actual Price_CAD from DB
              +  Distance_from_Edmonton_KM / Distance_from_Calgary_KM
              (distances are pre-stored in tbl_Locations by the ETL;
               inference uses real-time OSRM values instead)

Filters applied before training
  - Days_to_Sell >= 1   : exclude same-day sales (noise)
  - exclude listings whose Scrape_Date = first scrape day in the DB
    (those vehicles were already on the market before data collection
     started — their true listing date is unknown, so Days_to_Sell
     would be artificially short)

Output
  models/catboost_days_model.pkl   (sklearn Pipeline)

Run
  python training/train_days_catboost.py
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sqlalchemy import create_engine, text

from catboost import CatBoostRegressor
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DB_SERVER = "."
DB_NAME   = "AB_CarSale_DB"
DB_DRIVER = "ODBC+Driver+17+for+SQL+Server"

MODEL_PATH = Path(__file__).parent.parent / "models" / "catboost_days_model.pkl"

CV_FOLDS    = 5
RANDOM_SEED = 42

# Numeric features — Price_CAD and distances are known at training time
# because they come directly from the database (V_Y2 + tbl_Locations).
# At inference time these three values are supplied externally:
#   Price_CAD                  ← predicted by Model 1
#   Distance_from_Edmonton_KM  ← real-time OSRM  (see inference/distance_api.py)
#   Distance_from_Calgary_KM   ← real-time OSRM
NUM_FEATURES = [
    "Year", "Kilometres", "Price_CAD",
    "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM",
]
CAT_FEATURES = [
    "Condition_Label", "Transmission_Type", "Drivetrain_Type",
    "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim",
]
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET       = "Days_to_Sell"

# Indices of categorical columns after ColumnTransformer output
# (numeric columns come first, then categoricals — same order as ALL_FEATURES)
CAT_INDICES = list(range(len(NUM_FEATURES), len(NUM_FEATURES) + len(CAT_FEATURES)))

CATBOOST_PARAMS = {
    "iterations":            2000,
    "learning_rate":         0.05,
    "depth":                 8,
    "l2_leaf_reg":           3.0,
    "subsample":             0.8,
    "early_stopping_rounds": 50,   # stops early per fold — no manual tuning needed
    "loss_function":         "RMSE",
    "random_seed":           RANDOM_SEED,
    "verbose":               False,
}


# ─────────────────────────────────────────────────────────────────────────────
#  CATBOOST WRAPPER
#  sklearn's clone() is incompatible with CatBoostRegressor when cat_features
#  is set in the constructor.  This wrapper stores all params as plain
#  attributes so sklearn can safely clone it during cross-validation,
#  then builds the real CatBoostRegressor only at fit() time.
# ─────────────────────────────────────────────────────────────────────────────
class CatBoostWrapper(BaseEstimator, RegressorMixin):
    def __init__(
        self,
        iterations=2000,
        learning_rate=0.05,
        depth=8,
        l2_leaf_reg=3.0,
        subsample=0.8,
        early_stopping_rounds=50,
        cat_features=None,
        loss_function="RMSE",
        random_seed=42,
        verbose=False,
    ):
        self.iterations            = iterations
        self.learning_rate         = learning_rate
        self.depth                 = depth
        self.l2_leaf_reg           = l2_leaf_reg
        self.subsample             = subsample
        self.early_stopping_rounds = early_stopping_rounds
        self.cat_features          = cat_features
        self.loss_function         = loss_function
        self.random_seed           = random_seed
        self.verbose               = verbose

    def fit(self, X, y, eval_set=None):
        self.model_ = CatBoostRegressor(
            iterations=self.iterations,
            learning_rate=self.learning_rate,
            depth=self.depth,
            l2_leaf_reg=self.l2_leaf_reg,
            subsample=self.subsample,
            early_stopping_rounds=self.early_stopping_rounds,
            cat_features=self.cat_features,
            loss_function=self.loss_function,
            random_seed=self.random_seed,
            verbose=self.verbose,
        )
        self.model_.fit(X, y)
        return self

    def predict(self, X):
        return self.model_.predict(X)

    def get_feature_importance(self):
        return self.model_.get_feature_importance()


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 1 — LOAD
# ─────────────────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """
    Pull SOLD records from V_Y2 with two filters:
      1. Days_to_Sell >= 1  — remove same-day sales
      2. Exclude listings that appeared on the very first scrape date —
         their real market entry is unknown so Days_to_Sell is unreliable
    """
    engine = create_engine(
        f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}"
        f"?driver={DB_DRIVER}&trusted_connection=yes"
    )
    query = text("""
        SELECT *
        FROM   dbo.V_Y2
        WHERE  Days_to_Sell >= 1
          AND  Listing_ID NOT IN (
                   SELECT Listing_ID
                   FROM   tbl_Listing_Status
                   WHERE  Scrape_Date = (
                              SELECT MIN(Scrape_Date) FROM tbl_Listing_Status
                          )
               )
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    engine.dispose()
    print(f"[LOAD] {len(df):,} rows loaded from V_Y2")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2 — PREPARE
# ─────────────────────────────────────────────────────────────────────────────
def prepare(df: pd.DataFrame):
    """
    Select the required columns and drop rows where the target
    or distance columns are NULL (listings with no geocoded location).
    No log transform is applied — Days_to_Sell is not as skewed as price
    and the RMSE loss function handles the raw scale well.
    """
    df = df[ALL_FEATURES + [TARGET]].copy()

    before = len(df)
    df = df.dropna(subset=[TARGET, "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM"])
    dropped = before - len(df)
    if dropped:
        print(f"[PREPARE] Dropped {dropped} rows with NULL target or distances")

    X = df[ALL_FEATURES]
    y = df[TARGET].astype(float)
    print(f"[PREPARE] {len(X):,} rows ready  |  target range: "
          f"{y.min():.0f} – {y.max():.0f} days  |  median: {y.median():.0f} days")
    return X, y


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — BUILD PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline() -> Pipeline:
    """
    Preprocessing:
      Numeric    → median imputation  (handles the rare NULL distance)
      Categorical → fill NULL with 'UNKNOWN', passed as-is to CatBoost
                    (CatBoost handles raw strings natively — no OHE needed)
    """
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"),                         NUM_FEATURES),
            ("cat", SimpleImputer(strategy="constant", fill_value="UNKNOWN"), CAT_FEATURES),
        ]
    )
    model = CatBoostWrapper(
        **CATBOOST_PARAMS,
        cat_features=CAT_INDICES,
    )
    return Pipeline([
        ("preprocessor", preprocessor),
        ("model",        model),
    ])


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4 — CROSS-VALIDATE
# ─────────────────────────────────────────────────────────────────────────────
def evaluate(X: pd.DataFrame, y: pd.Series) -> None:
    """
    5-fold cross-validation.  Metrics reported in original days (no transform).
    MAE is the most interpretable metric here: on average the model is off
    by MAE days when predicting how long a listing will take to sell.
    """
    cv      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    metrics = {"R2": [], "MAE": [], "RMSE": []}

    print(f"\n{'─' * 55}")
    print(f"CatBoost — Days-to-Sell  |  {CV_FOLDS}-Fold Cross-Validation")
    print(f"{'─' * 55}")

    for fold, (train_idx, val_idx) in enumerate(cv.split(X), 1):
        X_tr,  X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr,  y_val = y.iloc[train_idx], y.iloc[val_idx]

        pipeline = build_pipeline()
        pipeline.fit(X_tr, y_tr)
        y_pred = pipeline.predict(X_val)

        metrics["R2"].append(r2_score(y_val, y_pred))
        metrics["MAE"].append(mean_absolute_error(y_val, y_pred))
        metrics["RMSE"].append(root_mean_squared_error(y_val, y_pred))

        print(f"  Fold {fold}:  R²={metrics['R2'][-1]:.4f}  "
              f"MAE={metrics['MAE'][-1]:.1f} days  "
              f"RMSE={metrics['RMSE'][-1]:.1f} days")

    print(f"{'─' * 55}")
    for name, vals in metrics.items():
        arr = np.array(vals)
        print(f"  CV {name:4s}: {arr.mean():.4f} ± {arr.std():.4f}")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 5 — FINAL FIT + FEATURE IMPORTANCE + SAVE
# ─────────────────────────────────────────────────────────────────────────────
def fit_final(X: pd.DataFrame, y: pd.Series) -> None:
    """
    Re-fit on the full dataset (all folds) and save the pipeline.
    Feature importances show which variables drive Days_to_Sell —
    useful to discuss during the demo.
    """
    print(f"\n[TRAIN] Fitting final pipeline on full dataset ...")
    pipeline = build_pipeline()
    pipeline.fit(X, y)

    imp = pd.Series(
        pipeline.named_steps["model"].get_feature_importance(),
        index=ALL_FEATURES,
    ).sort_values(ascending=False)

    print("\n  Feature importances (Days-to-Sell model):")
    print(imp.to_string())

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"\n[SAVE] Pipeline saved → {MODEL_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df       = load_data()
    X, y     = prepare(df)
    evaluate(X, y)
    fit_final(X, y)