# Model 2 — Days-to-Sell Prediction: What We Tried and Why We Stopped

## Background

After building Model 1 (price prediction), we wanted to answer a second question:
**"If I list a car today, how many days will it take to sell?"**

This would be useful for sellers who want to plan ahead. We called this Model 2.

---

## What We Did

### Step 1 — Regression (Predicting the exact number of days)

Our first approach was straightforward: treat `Days_to_Sell` as a number and train
a CatBoost regression model to predict it directly.

We used the following features:

- Vehicle attributes: make, model, trim, year, kilometres, body style, colour, etc.
- Actual sale price (`Price_CAD`) from the database
- Driving distance from Edmonton and Calgary (pre-stored in the database)

We filtered the data to only include:
- Listings with `Days_to_Sell >= 1` (removing same-day sales as noise)
- Listings that were **not** on the market on the very first day of scraping
  (because those cars were already listed before we started collecting data,
  so their true start date is unknown)

**Results:**

| Metric | Score |
|--------|-------|
| R² (avg across 5 folds) | 0.018 |
| MAE | 4.5 days |
| RMSE | 6.0 days |

R² = 0.018 means the model explains less than 2% of the variation in `Days_to_Sell`.
This is essentially the same as guessing the average every time. The model learned nothing useful.

### Step 2 — Classification (Fast sale vs. Slow sale)

We suspected the regression was failing because the target variable had very low
variance. So we reframed the problem as a binary classification:

> **Will this car sell within 7 days?**
> - Fast (1): `Days_to_Sell <= 7`
> - Slow (0): `Days_to_Sell > 7`

We chose 7 days as the threshold because it gave the most balanced class split
when we tested different values (5, 7, 10, 14 days).

We switched from `CatBoostRegressor` to `CatBoostClassifier`, changed the loss
function from RMSE to Logloss, and used Stratified K-Fold cross-validation to
preserve the class ratio in each fold.

**Results:**

| Metric | Score |
|--------|-------|
| Accuracy | 0.687 |
| F1 Score | 0.806 |
| ROC-AUC | 0.601 |

At first glance, Accuracy and F1 look acceptable. But these numbers are misleading.
When we looked more closely at what the model was actually predicting:

- **70% of our samples were Fast** — so a model that just predicts "Fast" for
  everything would already get 70% accuracy and a high F1 score automatically.
- The model's **recall for Slow was only 0.28**, meaning it correctly identified
  only 28% of slow-selling cars. It was essentially ignoring the Slow class entirely.
- **ROC-AUC = 0.601** — this is the most honest metric here. A score of 0.5
  means random guessing; 0.601 means our model is barely better than flipping a coin.

---

## Why the Model Failed

The core problem is not the algorithm — it is the data itself.

### Our data collection window was only ~5 weeks

The car listings were scraped from Kijiji by our team during the semester.
Due to time constraints, we only had about 5 weeks of data.

This created a hard ceiling on `Days_to_Sell`: **the maximum value in our dataset
is 36 days**, and 99% of values are below 35 days.

In reality, cars can sit on the market for months. But we never observed that —
any car that hadn't sold within our 5-week window simply appeared as still "Active"
and was excluded from the training data.

### The result: almost no variation in the target variable

| Statistic | Value |
|-----------|-------|
| Min | 1 day |
| Median | 6 days |
| Max | 36 days |
| Std deviation | 8.9 days |

When every car in your training data sells within 36 days, the model has
very little to distinguish between a "fast" and "slow" sale. The range is
just too narrow.

### The class imbalance made classification unreliable too

Because slow-selling cars (which would take months in real life) were never
observed, our "Slow" class only captured cars that sold in 8–36 days — a very
compressed range. This made 70% of our data fall into the Fast class, and the
model learned to predict Fast almost always.

---

## Decision: Drop Model 2

We decided to remove Model 2 from the final project for the following reasons:

1. **Both approaches failed for the same underlying reason** — the training data
   does not reflect the true distribution of `Days_to_Sell` in the real market.

2. **Reporting misleading metrics would be dishonest.** Accuracy = 0.69 and
   F1 = 0.81 sound good but are inflated purely by class imbalance.
   The true signal (AUC = 0.60) shows the model has almost no predictive power.

3. **The fix requires more data, not a better model.** To make this prediction
   meaningful, we would need at least 6–12 months of listing data to observe
   the full range of selling timelines. That is beyond the scope of a single semester.

---

## What Would Make Model 2 Work

If this project were continued in a future semester, the following changes would
make Model 2 viable:

- **Scrape continuously for 6–12 months** to capture listings that take weeks
  or months to sell
- This would produce a much wider and more realistic `Days_to_Sell` distribution
- With enough slow-selling examples, both regression and classification would
  have something meaningful to learn from

---

## Summary

| Approach | Key Metric | Conclusion |
|----------|-----------|------------|
| Regression | R² = 0.018 | No predictive power |
| Classification (threshold = 7 days) | AUC = 0.601 | Barely above random |
| **Decision** | — | **Model 2 dropped** |

The data collection window was the limiting factor. This is a known constraint
of scraping-based projects and is not a reflection of the model design or
feature selection.