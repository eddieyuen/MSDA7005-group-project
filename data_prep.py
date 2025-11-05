import json
import pandas as pd

# read JSON file
with open('tps_sale_transactions_en.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# choose the list of record dicts
records = data['records']

# flatten nested dicts into columns
df = pd.json_normalize(records)

# Apply transformations to specific columns
## Convert string to float
df['estate_map_latitude'] = df['estate_map_latitude'].apply(lambda x: float(x) if x else None)
df['estate_map_longitude'] = df['estate_map_longitude'].apply(lambda x: float(x) if x else None)
df['transaction_price'] = df['transaction_price'].apply(lambda x: float(x) if x else None)
df['discount_rate'] = df['discount_rate'].apply(lambda x: float(x) if x else None)

## Split variables with mixed units into two separate columns
df['saleable_floor_area_per_sq_m'] = df['saleable_floor_area'].apply(lambda x: float(x.split('/')[0]) if x else None)
df['saleable_floor_area_per_sq_ft'] = df['saleable_floor_area'].apply(lambda x: float(x.split('/')[1]) if x else None)
df['transaction_price_per_sq_m'] = df['transaction_price_per_sq'].apply(lambda x: float(x.split('/')[0]) if x else None)
df['transaction_price_per_sq_ft'] = df['transaction_price_per_sq'].apply(lambda x: float(x.split('/')[1]) if x else None)

# Convert the df to CSV and Excel formats
df.to_csv('tps_sale_transactions_en.csv', index=False, encoding='utf-8')
df.to_excel('tps_sale_transactions_en.xlsx', index=False) 