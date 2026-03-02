# Used Car Optimal Price Prediction System
[cite_start]**A Data-Driven Decision Engine for Secondary Auto Markets** [cite: 1, 4]

## 📌 Project Overview
[cite_start]This system provides sellers with a competitive edge by predicting the **Optimal Listing Price** and **Estimated Time-to-Sell**[cite: 4, 5]. [cite_start]By analyzing real-time market data scraped from Kijiji, the platform generates three distinct selling strategies (Quick Sale, Max Profit, and Balanced) to align with diverse user needs[cite: 6, 11].

## 🏗️ Technical Architecture

### 1. The Data Fortress (SQL Server)
[cite_start]The backend is built on a **3NF normalized schema** to ensure data integrity and eliminate redundancy[cite: 14, 38].
* [cite_start]**Core Entities**: Includes Tables for Listings, Vehicles, Locations, Conditions, Models, Body Styles, Transmissions, Colours, Drivetrains, and Seats[cite: 9].
* [cite_start]**Integrity**: Enforced through custom `CHECK` constraints and strict relational mapping[cite: 38, 44].

### 2. The Intelligence Engine (Python)
[cite_start]Two interconnected machine learning models drive the decision logic[cite: 4, 5]:
* [cite_start]**Price Model**: Estimates fair market value based on vehicle features[cite: 4].
* [cite_start]**Time Model**: Predicts market duration using vehicle features and the predicted price[cite: 5].
* [cite_start]**Key Predictors (X)**: Kilometres, Model, Transmission, Colour, Seats, Body Style, Drivetrain, Status, Location, and Condition[cite: 13].

## 💼 Business Use Cases
1. [cite_start]**Individual Sellers**: Optimize personal sales based on urgency or profit[cite: 17, 18].
2. [cite_start]**Dealerships**: Manage inventory turnover and maximize cash flow[cite: 21, 22].
3. [cite_start]**Buyers**: Identify fair pricing to avoid overpaying[cite: 24, 25].
4. [cite_start]**Marketplace Platforms**: Enhance user experience and transaction success rates[cite: 27, 28].
5. [cite_start]**Insurance Companies**: Establish accurate vehicle valuations for premium setting[cite: 30, 32].

## 👥 Team & Roles
* [cite_start]**Miles Zhang (Lead Data Engineer)**: Database architecture, 3NF normalization, and integrity constraints[cite: 36, 38].
* [cite_start]**Chen Liu (Lead Data Scientist)**: ETL pipeline development, feature engineering, and model tuning[cite: 39, 41, 46].
