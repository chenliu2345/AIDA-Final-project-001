import pandas as pd
df = pd.read_csv('Alberta_owner_sales_car.csv') 
df_raw = df.copy()
cols_to_fix = [col for col in ['Condition', 'Transmission', 'Drivetrain', 'Seats', 'Body Style', 'Colour', 'Model'] if col in df.columns]
for col in cols_to_fix:
    df[col] = df[col].fillna('Unknown')
print("Cheak header 5：")
print(df[cols_to_fix].head())
df.to_csv('Alberta_owner_sales_car_clean.csv', index=False)
print("save as 'Alberta_owner_sales_car_clean.csv'")