import pandas as pd

# ── Load data ──────────────────────────────────────────────────────────────
df_sold   = pd.read_csv('Final_Sold_Data.csv',   encoding='utf-8-sig', low_memory=False)
df_listed = pd.read_csv('Final_Listed_Data.csv', encoding='utf-8-sig', low_memory=False)

# ═══════════════════════════════════════════════════════════════════════════
# 1. SHAPE
# ═══════════════════════════════════════════════════════════════════════════
print("=== SHAPE ===")
print(f"df_sold:   {df_sold.shape[0]:,} rows x {df_sold.shape[1]} columns")
print(f"df_listed: {df_listed.shape[0]:,} rows x {df_listed.shape[1]} columns")

# ═══════════════════════════════════════════════════════════════════════════
# 2. DUPLICATES
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== DUPLICATES ===")
print(f"df_sold   duplicate rows: {df_sold.duplicated().sum():,}")
print(f"df_listed duplicate rows: {df_listed.duplicated().sum():,}")

# Duplicates by ListingKey specifically (more meaningful than full row)
print(f"df_sold   duplicate ListingKeys: {df_sold['ListingKey'].duplicated().sum():,}")
print(f"df_listed duplicate ListingKeys: {df_listed['ListingKey'].duplicated().sum():,}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. MISSING VALUES
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== MISSING VALUES (%) — df_sold ===")
sold_missing = (df_sold.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(sold_missing[sold_missing > 0].to_string())

print("\n=== MISSING VALUES (%) — df_listed ===")
listed_missing = (df_listed.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(listed_missing[listed_missing > 0].to_string())

# ═══════════════════════════════════════════════════════════════════════════
# 4. BASIC STATS — key numeric columns
# ═══════════════════════════════════════════════════════════════════════════
sold_num_cols   = ['ClosePrice', 'OriginalListPrice', 'ListPrice', 'DaysOnMarket', 'LivingArea', 'BedroomsTotal', 'BathroomsTotalInteger']
listed_num_cols = ['OriginalListPrice', 'ListPrice', 'DaysOnMarket', 'LivingArea', 'BedroomsTotal', 'BathroomsTotalInteger']

print("\n=== BASIC STATS — df_sold ===")
print(df_sold[sold_num_cols].agg(['mean', 'median', 'min', 'max']).round(2).to_string())

print("\n=== BASIC STATS — df_listed ===")
print(df_listed[listed_num_cols].agg(['mean', 'median', 'min', 'max']).round(2).to_string())

# ═══════════════════════════════════════════════════════════════════════════
# 5. CATEGORICAL OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
print("\n=== CATEGORICAL OVERVIEW — df_sold ===")
for col in ['PropertyType', 'PropertySubType', 'StateOrProvince']:
    print(f"\n{col}:\n{df_sold[col].value_counts().head(10).to_string()}")

print("\n=== CATEGORICAL OVERVIEW — df_listed ===")
for col in ['PropertyType', 'MlsStatus']:
    if col in df_listed.columns:
        print(f"\n{col}:\n{df_listed[col].value_counts().head(10).to_string()}")

print("\nEDA complete!")
