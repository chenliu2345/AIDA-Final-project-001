# Technical Decision: Model Optimization & Feature Selection

## 1. Feature Ablation Study: `Unknown_Score` Impact
Comparing the model performance with and without the `Unknown_Score` feature reveals that its inclusion does not provide predictive gains.

| Metric | With `Unknown_Score` | Without `Unknown_Score` | Result |
| :--- | :--- | :--- | :--- |
| **CV R²** | **0.7109** | **0.7117** | 🟢 Improved without feature |
| **CV MAE** | $4,287 | $4,277 | 🟢 Lower error without feature |
| **CV RMSE** | $8,873 | $8,861 | 🟢 Lower error without feature |

---

## 2. Current Model Performance (Detailed)
The following results represent the Random Forest regressor trained on **18,024 rows** from `dbo.V_Y1` (current version with `Unknown_Score`).

| Metric | 5-Fold CV (Mean ± Std) | Final Model (Full Train) |
| :--- | :--- | :--- |
| **R²** | **0.7109** ± 0.0235 | 0.8787 |
| **MAE** | 4286.5380 ± 80.4770 | 2503.88 |
| **RMSE** | 8873.3962 ± 566.1383 | 5750.08 |

---

## 3. Feature Importance (Top 10)
`Year` remains the dominant predictor, while `Unknown_Score` ranks last.

1. **Year**: 0.5579
2. **Kilometres**: 0.1363
3. **Body_Style**: 0.1112
4. **Base_Model**: 0.0792
5. **Trim**: 0.0310
6. **City_Name**: 0.0251
7. **Drivetrain_Type**: 0.0208
8. **Colour**: 0.0141
9. **Seats_Count**: 0.0093
10. **Unknown_Score**: 0.0089

---

## 4. Final Decision: Removal of `Unknown_Score`
Based on the metrics above, I have decided to **discard** the `Unknown_Score` feature for the following reasons:

* **Performance Degradation:** Removing the feature actually **improves the CV R² from 0.7109 to 0.7117** and reduces both MAE and RMSE. This confirms that the feature acts as noise rather than a signal.
* **Redundancy:** The existing features (Year, Kilometres, etc.) already sufficiently capture the underlying data patterns. `Unknown_Score` provides no additional unique information.
* **Model Simplicity:** In accordance with Occam's Razor, we should prefer the simpler model that yields better performance.