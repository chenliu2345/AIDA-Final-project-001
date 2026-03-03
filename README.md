# Used Car Optimal Price Prediction System
**A Data-Driven Decision Engine for Secondary Auto Markets**

## 📌 Project Overview
This system provides sellers with a competitive edge by predicting the **Optimal Listing Price** and **Estimated Time-to-Sell**. By analyzing real-time market data scraped from Kijiji, the platform generates three distinct selling strategies (Quick Sale, Max Profit, and Balanced) to align with diverse user needs.

## 🏗️ Technical Architecture

### 1. The Data Fortress (SQL Server)
The backend is built on a **3NF normalized schema** to ensure data integrity and eliminate redundancy.
* **Core Entities**: Includes Tables for Listings, Vehicles, Locations, Conditions, Models, Body Styles, Transmissions, Colours, Drivetrains, and Seats.
* **Integrity**: Enforced through custom `CHECK` constraints and strict relational mapping.

### 2. The Intelligence Engine (Python)
Two interconnected machine learning models drive the decision logic:
* **Price Model**: Estimates fair market value based on vehicle features.
* **Time Model**: Predicts market duration using vehicle features and the predicted price.
* **Key Predictors (X)**: Kilometres, Model, Transmission, Colour, Seats, Body Style, Drivetrain, Status, Location, and Condition.

## 💼 Business Use Cases
1. **Individual Sellers**: Optimize personal sales based on urgency or profit.
2. **Dealerships**: Manage inventory turnover and maximize cash flow.
3. **Buyers**: Identify fair pricing to avoid overpaying.
4. **Marketplace Platforms**: Enhance user experience and transaction success rates.
5. **Insurance Companies**: Establish accurate vehicle valuations for premium setting.

## 👥 Team & Roles
* **Miles Zhang (Lead Data Engineer)**: Database architecture, 3NF normalization, and integrity constraints.
* **Chen Liu (Lead Data Scientist)**: ETL pipeline development, feature engineering, and model tuning.

## Disclaimer
This project is for learning and research purposes only. Any commercial use is strictly prohibited, and I am not responsible for any disputes arising from its use.

## Academic Use Only
All data collected through this project is utilized exclusively for a group assignment within the AI and Data Analytics (AIDA) program at Red Deer Polytechnic. No data is shared with third parties or used for commercial gain.
