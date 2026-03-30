import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

engine = create_engine(
    "mssql+pyodbc://./AB_CarSale_DB"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
with engine.connect() as conn:
    df = pd.read_sql(text("SELECT Price_CAD FROM dbo.V_Y1 WHERE Status_Label = 'SOLD'"), conn)
engine.dispose()

print(df["Price_CAD"].describe())
df["Price_CAD"].hist(bins=50)
plt.xlabel("Price_CAD")
plt.title("Price Distribution")
plt.tight_layout()
plt.show()