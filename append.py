import pandas as pd
import glob

def read_csv_safe(filepath):
    for encoding in ['utf-8-sig', 'latin-1', 'cp1252']:
        try:
            return pd.read_csv(filepath, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not read {filepath} with any encoding")

print("Starting to merge Sold data...")
sold_files = glob.glob('raw/CRMLSSold*.csv')
df_sold = pd.concat((read_csv_safe(f) for f in sold_files), ignore_index=True)
print(f"Sold data merged successfully! Total rows: {len(df_sold)}. Exporting...")
df_sold.to_csv('Final_Sold_Data.csv', index=False, encoding='utf-8-sig')
print("Final Sold table saved as Final_Sold_Data.csv")

print("Starting to merge Listed data...")
listed_files = glob.glob('raw/CRMLSListing*.csv')
df_listed = pd.concat((read_csv_safe(f) for f in listed_files), ignore_index=True)
print(f"Listed data merged successfully! Total rows: {len(df_listed)}. Exporting...")
df_listed.to_csv('Final_Listed_Data.csv', index=False, encoding='utf-8-sig')
print("Final Listed table saved as Final_Listed_Data.csv")
