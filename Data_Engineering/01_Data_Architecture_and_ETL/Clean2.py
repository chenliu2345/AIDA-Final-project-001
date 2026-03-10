import pandas as pd
import numpy as np
df_raw = pd.read_csv('Alberta_owner_sales_car_clean.csv') 
df_cleaned = df_raw.copy()
df_cleaned['Year'] = df_cleaned['Model'].str.extract(r'^(\d{4})')
df_cleaned['Year'] = pd.to_numeric(df_cleaned['Year'], errors='coerce')
df_cleaned['Trim'] = df_cleaned['Model'].str.extract(r',\s*(.*)$')
df_cleaned['Trim'] = df_cleaned['Trim'].fillna('UNKNOWN').str.strip().str.upper()
df_cleaned['Base_Model'] = df_cleaned['Model'].str.extract(r'^\d{4}\s+(.*?)(?:,|$)')
df_cleaned['Base_Model'] = df_cleaned['Base_Model'].str.strip().str.upper()
df_cleaned = df_cleaned.drop(columns=['Model'])
binning_config = {
    'Base_Model': 10,  
    'Trim': 10,
    'Location': 10
}
for col, threshold in binning_config.items():
    if col in df_cleaned.columns:
        counts = df_cleaned[col].value_counts()
        to_keep = counts[counts >= threshold].index
        df_cleaned[col] = df_cleaned[col].where(df_cleaned[col].isin(to_keep), 'OTHER')
        print(f"   - {col:<12}: more than {threshold}, keep {len(to_keep)} rows")
categorical_cols = ['Transmission', 'Drivetrain', 'Seats', 'Body Style', 'Colour', 'Condition']
for col in categorical_cols:
    if col in df_cleaned.columns:
        df_cleaned[col] = df_cleaned[col].fillna('UNKNOWN').str.strip().str.upper()
print("Finish")
output_name = 'Optimized_Alberta_owner_sales_car_clean.csv'
df_cleaned.to_csv(output_name, index=False)

print(f"{output_name}' in there！")