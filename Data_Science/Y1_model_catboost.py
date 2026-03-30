"""
Model 1 — CatBoost: Optimal Price Prediction
Data  : V_Y1 WHERE Status_Label = 'SOLD'
Tuning: Fixed hyperparameters with early stopping (no Optuna needed)
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sqlalchemy import create_engine, text

from catboost import CatBoostRegressor, Pool
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DB_SERVER  = "."
DB_NAME    = "AB_CarSale_DB"
DB_DRIVER  = "ODBC+Driver+17+for+SQL+Server"

MODEL_PATH = Path(__file__).parent.parent / "models" / "catboost_price_model.pkl"

CV_FOLDS    = 5
RANDOM_SEED = 42

NUM_FEATURES = ["Year", "Kilometres"]
CAT_FEATURES = [
    "Condition_Label", "Transmission_Type", "Drivetrain_Type",
    "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim",
]
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET       = "Price_CAD"

# Categorical column indices after ColumnTransformer output:
# order is [NUM_FEATURES..., CAT_FEATURES...]
CAT_INDICES = list(range(len(NUM_FEATURES), len(NUM_FEATURES) + len(CAT_FEATURES)))

# ─────────────────────────────────────────────────────────────────────────────
#  CATBOOST HYPERPARAMETERS
#  CatBoost performs well with these defaults. early_stopping_rounds
#  automatically finds the optimal number of iterations per fold,
#  removing the need for Optuna tuning.
# ─────────────────────────────────────────────────────────────────────────────
CATBOOST_PARAMS = {
    "iterations":            2000,
    "learning_rate":         0.05,
    "depth":                 8,
    "l2_leaf_reg":           3.0,
    "subsample":             0.8,
    "early_stopping_rounds": 50,
    "loss_function":         "RMSE",
    "random_seed":           RANDOM_SEED,
    "verbose":               False,
}


# ─────────────────────────────────────────────────────────────────────────────
#  CATBOOST WRAPPER
#
#  sklearn's clone() is incompatible with CatBoostRegressor when cat_features
#  is set in the constructor. This wrapper stores all params as plain
#  attributes that sklearn can safely clone, then builds the real
#  CatBoostRegressor at fit time.
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
    df = df.dropna(subset=[TARGET])
    X = df[ALL_FEATURES]
    y = df[TARGET].astype(float)
    return X, y


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — BUILD PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline() -> Pipeline:
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
    cv      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    metrics = {"R2": [], "MAE": [], "RMSE": []}

    print(f"\n{'─'*55}")
    print(f"CatBoost — {CV_FOLDS}-Fold Cross-Validation")
    print(f"{'─'*55}")

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
              f"MAE={metrics['MAE'][-1]:.0f}  "
              f"RMSE={metrics['RMSE'][-1]:.0f}")

    print(f"{'─'*55}")
    for name, vals in metrics.items():
        arr = np.array(vals)
        print(f"  CV {name:4s}: {arr.mean():.4f} ± {arr.std():.4f}")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 5 — FINAL FIT + FEATURE IMPORTANCE + SAVE
# ─────────────────────────────────────────────────────────────────────────────
def fit_final(X: pd.DataFrame, y: pd.Series) -> None:
    print(f"\nFitting final pipeline on full dataset ...")
    pipeline = build_pipeline()
    pipeline.fit(X, y)

    imp = pd.Series(
        pipeline.named_steps["model"].get_feature_importance(),
        index=ALL_FEATURES,
    ).sort_values(ascending=False)

    print("\n  Top 10 Feature Importances:")
    print(imp.head(10).to_string())

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