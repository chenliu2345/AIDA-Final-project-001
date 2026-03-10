import pandas as pd
file_path = 'Optimized_Alberta_owner_sales_car_clean.csv' 
df = pd.read_csv(file_path)
categorical_cols = [
    'Location',       
    'Status',         
    'Condition',      
    'Transmission', 
    'Drivetrain', 
    'Seats', 
    'Body Style', 
    'Colour',	
    'Trim',
    'Base_Model'
]
total_unique_columns = 0
for col in categorical_cols:
    if col in df.columns:
        unique_count = df[col].nunique()
        total_unique_columns += unique_count
        print(f"【{col}】")
        print(f"unique numbers: {unique_count}")
        print(f"【{col}】 Unique: {df[col].nunique()}\n{df[col].value_counts().head(10).to_string()}")
print(f"There have {len(categorical_cols)} columns, {total_unique_columns} matrices。")