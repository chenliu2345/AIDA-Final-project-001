"""
Model 2 — CatBoost: Days-to-Sell Classification   (training only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Data source : V_Y2  (SOLD records with Days_to_Sell)
Task        : Binary classification
Target      : Fast_Sale  =  1 if Days_to_Sell <= 7, else 0

Why classification instead of regression?
  The raw Days_to_Sell range is capped at 36 days because the scraping
  window was ~5 weeks.  That truncated range produces near-zero variance
  in the target, making R² meaningless for a regressor.  Framing the
  problem as "will this car sell within a week?" is both statistically
  valid and practically useful for a seller.

Threshold = 7 days chosen because it gives the most balanced classes:
  Fast (<=7 days) : ~55.6%   Slow (>7 days) : ~44.4%

Features    : vehicle attributes  +  actual Price_CAD from DB
              +  Distance_from_Edmonton_KM / Distance_from_Calgary_KM
              (distances are pre-stored in tbl_Locations by the ETL;
               inference uses real-time OSRM values instead)

Filters applied before training
  - Days_to_Sell >= 1   : exclude same-day sales (noise)
  - exclude listings whose Scrape_Date = first scrape day in the DB
    (those vehicles were on the market before data collection started —
     their true listing date is unknown so Days_to_Sell is unreliable)

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

from catboost import CatBoostClassifier
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    roc_auc_score,
    classification_report,
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DB_SERVER = "."
DB_NAME   = "AB_CarSale_DB"
DB_DRIVER = "ODBC+Driver+17+for+SQL+Server"

MODEL_PATH = Path(__file__).parent.parent / "models" / "catboost_days_model.pkl"

FAST_SALE_THRESHOLD = 7   # days — sell within a week = Fast (1), otherwise Slow (0)
CV_FOLDS            = 5
RANDOM_SEED         = 42

NUM_FEATURES = [
    "Year", "Kilometres", "Price_CAD",
    "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM",
]
CAT_FEATURES = [
    "Condition_Label", "Transmission_Type", "Drivetrain_Type",
    "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim",
]
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET       = "Fast_Sale"   # engineered from Days_to_Sell

CAT_INDICES = list(range(len(NUM_FEATURES), len(NUM_FEATURES) + len(CAT_FEATURES)))

CATBOOST_PARAMS = {
    "iterations":            2000,
    "learning_rate":         0.05,
    "depth":                 8,
    "l2_leaf_reg":           3.0,
    "subsample":             0.8,
    "early_stopping_rounds": 50,
    "loss_function":         "Logloss",   # binary classification loss
    "eval_metric":           "AUC",
    "random_seed":           RANDOM_SEED,
    "verbose":               False,
}


# ─────────────────────────────────────────────────────────────────────────────
#  CATBOOST WRAPPER
#  sklearn's clone() is incompatible with CatBoostClassifier when cat_features
#  is set in the constructor.  This wrapper stores all params as plain
#  attributes so sklearn can safely clone it during cross-validation,
#  then builds the real CatBoostClassifier only at fit() time.
# ─────────────────────────────────────────────────────────────────────────────
class CatBoostWrapper(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        iterations=2000,
        learning_rate=0.05,
        depth=8,
        l2_leaf_reg=3.0,
        subsample=0.8,
        early_stopping_rounds=50,
        cat_features=None,
        loss_function="Logloss",
        eval_metric="AUC",
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
        self.eval_metric           = eval_metric
        self.random_seed           = random_seed
        self.verbose               = verbose

    def fit(self, X, y, eval_set=None):
        self.model_ = CatBoostClassifier(
            iterations=self.iterations,
            learning_rate=self.learning_rate,
            depth=self.depth,
            l2_leaf_reg=self.l2_leaf_reg,
            subsample=self.subsample,
            early_stopping_rounds=self.early_stopping_rounds,
            cat_features=self.cat_features,
            loss_function=self.loss_function,
            eval_metric=self.eval_metric,
            random_seed=self.random_seed,
            verbose=self.verbose,
        )
        self.model_.fit(X, y)
        return self

    def predict(self, X):
        return self.model_.predict(X)

    def predict_proba(self, X):
        return self.model_.predict_proba(X)

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
    Engineer the binary target and drop rows with missing distances.
    Fast_Sale = 1  if Days_to_Sell <= FAST_SALE_THRESHOLD (7 days)
    Fast_Sale = 0  otherwise
    """
    df = df[ALL_FEATURES + ["Days_to_Sell"]].copy()

    before = len(df)
    df = df.dropna(subset=["Days_to_Sell", "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM"])
    dropped = before - len(df)
    if dropped:
        print(f"[PREPARE] Dropped {dropped} rows with NULL target or distances")

    df[TARGET] = (df["Days_to_Sell"] <= FAST_SALE_THRESHOLD).astype(int)

    fast = df[TARGET].sum()
    slow = len(df) - fast
    print(f"[PREPARE] {len(df):,} rows ready")
    print(f"          Fast (<=7 days) : {fast:,} ({fast/len(df)*100:.1f}%)")
    print(f"          Slow (> 7 days) : {slow:,} ({slow/len(df)*100:.1f}%)")

    X = df[ALL_FEATURES]
    y = df[TARGET]
    return X, y


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — BUILD PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline() -> Pipeline:
    """
    Preprocessing:
      Numeric    → median imputation
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
    Stratified 5-fold CV — StratifiedKFold preserves the Fast/Slow ratio
    in every fold, which matters when classes are not perfectly balanced.

    Metrics:
      Accuracy  — overall correctness
      F1        — harmonic mean of precision and recall (robust to imbalance)
      ROC-AUC   — model's ability to rank Fast above Slow regardless of threshold
    """
    cv      = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    metrics = {"Accuracy": [], "F1": [], "AUC": []}

    print(f"\n{'─' * 55}")
    print(f"CatBoost — Fast Sale Classifier  |  {CV_FOLDS}-Fold CV")
    print(f"Threshold : Days_to_Sell <= {FAST_SALE_THRESHOLD} days = Fast (1)")
    print(f"{'─' * 55}")

    for fold, (train_idx, val_idx) in enumerate(cv.split(X, y), 1):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        pipeline = build_pipeline()
        pipeline.fit(X_tr, y_tr)

        y_pred      = pipeline.predict(X_val)
        y_pred_prob = pipeline.predict_proba(X_val)[:, 1]

        metrics["Accuracy"].append(accuracy_score(y_val, y_pred))
        metrics["F1"].append(f1_score(y_val, y_pred))
        metrics["AUC"].append(roc_auc_score(y_val, y_pred_prob))

        print(f"  Fold {fold}:  "
              f"Accuracy={metrics['Accuracy'][-1]:.4f}  "
              f"F1={metrics['F1'][-1]:.4f}  "
              f"AUC={metrics['AUC'][-1]:.4f}")

    print(f"{'─' * 55}")
    for name, vals in metrics.items():
        arr = np.array(vals)
        print(f"  CV {name:8s}: {arr.mean():.4f} ± {arr.std():.4f}")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 5 — FINAL FIT + FEATURE IMPORTANCE + SAVE
# ─────────────────────────────────────────────────────────────────────────────
def fit_final(X: pd.DataFrame, y: pd.Series) -> None:
    """
    Re-fit on the full dataset and save the pipeline.
    Prints a classification report and feature importances for the demo.
    """
    print(f"\n[TRAIN] Fitting final pipeline on full dataset ...")
    pipeline = build_pipeline()
    pipeline.fit(X, y)

    y_pred = pipeline.predict(X)
    print("\n  Classification report (training set — for reference only):")
    print(classification_report(y, y_pred, target_names=["Slow (0)", "Fast (1)"]))

    imp = pd.Series(
        pipeline.named_steps["model"].get_feature_importance(),
        index=ALL_FEATURES,
    ).sort_values(ascending=False)

    print("  Feature importances (Fast-Sale classifier):")
    print(imp.to_string())

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"\n[SAVE] Pipeline saved → {MODEL_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df   = load_data()
    X, y = prepare(df)
    evaluate(X, y)
    fit_final(X, y)