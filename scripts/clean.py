import pandas as pd

# Load the CSV file
df = pd.read_csv('../data/olist_products_dataset.csv')

# Fill missing values for each column
df.loc[:, 'product_name_lenght'] = df['product_name_lenght'].fillna(0)
df.loc[:, 'product_photos_qty'] = df['product_photos_qty'].fillna(0)
df.loc[:, 'product_weight_g'] = df['product_weight_g'].fillna(0)

# Convert columns to appropriate data types
df['product_name_lenght'] = df['product_name_lenght'].astype(int)
df['product_photos_qty'] = df['product_photos_qty'].astype(int)
df['product_weight_g'] = df['product_weight_g'].astype(int)

# Save the cleaned data back to a CSV file
df.to_csv('cleaned_olist_products_dataset.csv', index=False)