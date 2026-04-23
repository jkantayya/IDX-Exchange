import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

LISTING_FILE = BASE_DIR / "Listing" / "CRMLSListing_filtered_residential.csv"
SOLD_FILE = BASE_DIR / "Sold" / "CRMLSSold_filtered_residential.csv"

DATE_COLS = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate",
]

NUMERIC_COLS = [
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "LivingArea",
    "LotSizeAcres",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "YearBuilt",
    "Latitude",
    "Longitude",
]

KEEP_COLS = [
    "ListingKey",
    "PropertyType",
    "PropertySubType",
    "City",
    "CountyOrParish",
    "PostalCode",
    "StateOrProvince",
    "MLSAreaMajor",
    "ListOfficeName",
    "BuyerOfficeName",
    "source_file",
] + DATE_COLS + NUMERIC_COLS

OPTIONAL_DROP_COLS = [
    "CloseDate.1",
    "BuyerOfficeName.1",
]

def safe_read_csv(path: Path):
    # first read just header so we know what exists
    header_df = pd.read_csv(path, encoding="utf-8-sig", nrows=0)
    available_cols = header_df.columns.tolist()

    usecols = [col for col in KEEP_COLS if col in available_cols]
    extra_optional = [col for col in OPTIONAL_DROP_COLS if col in available_cols]
    usecols = usecols + extra_optional

    print(f"Reading {len(usecols)} columns from {path.name}")
    print("Columns being loaded:")
    print(usecols)

    return pd.read_csv(
    path,
    encoding="utf-8-sig",
    usecols=usecols,
    low_memory=False,
    on_bad_lines="skip",
)

def clean_dataset(input_file: Path, output_file: Path, dataset_name: str):
    print("=" * 100)
    print(f"Cleaning dataset: {dataset_name}")
    print(f"Input: {input_file}")

    df = safe_read_csv(input_file)
    rows_before = len(df)
    cols_before = df.shape[1]

    print(f"Rows before cleaning: {rows_before}")
    print(f"Columns before cleaning: {cols_before}")

    # --------------------------------------------------
    # 1. Drop redundant columns if present
    # --------------------------------------------------
    drop_cols = [col for col in OPTIONAL_DROP_COLS if col in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
        print("\nDropped redundant columns:")
        for col in drop_cols:
            print(f" - {col}")
    else:
        print("\nNo redundant columns dropped.")

    # --------------------------------------------------
    # 2. Convert date fields
    # --------------------------------------------------
    print("\nConverting date columns...")
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            print(f" - Converted {col} to datetime")

    # --------------------------------------------------
    # 3. Ensure numeric fields are numeric
    # --------------------------------------------------
    print("\nConverting numeric columns...")
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            print(f" - Converted {col} to numeric")

    # --------------------------------------------------
    # 4. Invalid numeric flags required by handbook
    # --------------------------------------------------
    print("\nCreating invalid numeric flag columns...")

    if "ClosePrice" in df.columns:
        df["invalid_closeprice_flag"] = df["ClosePrice"] <= 0
    else:
        df["invalid_closeprice_flag"] = False

    if "LivingArea" in df.columns:
        df["invalid_livingarea_flag"] = df["LivingArea"] <= 0
    else:
        df["invalid_livingarea_flag"] = False

    if "DaysOnMarket" in df.columns:
        df["invalid_daysonmarket_flag"] = df["DaysOnMarket"] < 0
    else:
        df["invalid_daysonmarket_flag"] = False

    if "BedroomsTotal" in df.columns:
        df["invalid_bedrooms_flag"] = df["BedroomsTotal"] < 0
    else:
        df["invalid_bedrooms_flag"] = False

    if "BathroomsTotalInteger" in df.columns:
        df["invalid_bathrooms_flag"] = df["BathroomsTotalInteger"] < 0
    else:
        df["invalid_bathrooms_flag"] = False

    # --------------------------------------------------
    # 5. Date consistency flags required by handbook
    # --------------------------------------------------
    print("\nCreating date consistency flags...")

    if {"ListingContractDate", "CloseDate"}.issubset(df.columns):
        df["listing_after_close_flag"] = (
            df["ListingContractDate"].notna()
            & df["CloseDate"].notna()
            & (df["ListingContractDate"] > df["CloseDate"])
        )
    else:
        df["listing_after_close_flag"] = False

    if {"PurchaseContractDate", "CloseDate"}.issubset(df.columns):
        df["purchase_after_close_flag"] = (
            df["PurchaseContractDate"].notna()
            & df["CloseDate"].notna()
            & (df["PurchaseContractDate"] > df["CloseDate"])
        )
    else:
        df["purchase_after_close_flag"] = False

    if {"ListingContractDate", "PurchaseContractDate"}.issubset(df.columns):
        df["negative_timeline_flag"] = (
            df["ListingContractDate"].notna()
            & df["PurchaseContractDate"].notna()
            & (df["PurchaseContractDate"] < df["ListingContractDate"])
        )
    else:
        df["negative_timeline_flag"] = False

    # --------------------------------------------------
    # 6. Geographic data checks required by handbook
    # --------------------------------------------------
    print("\nCreating geographic quality flags...")

    has_lat = "Latitude" in df.columns
    has_lon = "Longitude" in df.columns

    df["missing_coordinate_flag"] = False
    df["zero_coordinate_flag"] = False
    df["positive_longitude_flag"] = False
    df["implausible_coordinate_flag"] = False

    if has_lat and has_lon:
        df["missing_coordinate_flag"] = df["Latitude"].isna() | df["Longitude"].isna()
        df["zero_coordinate_flag"] = (df["Latitude"] == 0) | (df["Longitude"] == 0)
        df["positive_longitude_flag"] = df["Longitude"] > 0

        # broad California-ish plausibility bounds
        df["implausible_coordinate_flag"] = (
            df["Latitude"].notna()
            & df["Longitude"].notna()
            & (
                (df["Latitude"] < 32) | (df["Latitude"] > 43) |
                (df["Longitude"] < -125) | (df["Longitude"] > -114)
            )
        )

    # --------------------------------------------------
    # 7. Create one overall invalid record flag
    # --------------------------------------------------
    flag_cols = [
        "invalid_closeprice_flag",
        "invalid_livingarea_flag",
        "invalid_daysonmarket_flag",
        "invalid_bedrooms_flag",
        "invalid_bathrooms_flag",
        "listing_after_close_flag",
        "purchase_after_close_flag",
        "negative_timeline_flag",
        "missing_coordinate_flag",
        "zero_coordinate_flag",
        "positive_longitude_flag",
        "implausible_coordinate_flag",
    ]

    df["any_data_quality_flag"] = df[flag_cols].any(axis=1)

    # --------------------------------------------------
    # 8. Cleaned analysis-ready dataset
    # Keep rows, but remove clearly invalid numeric records
    # --------------------------------------------------
    clean_df = df[
        ~df["invalid_closeprice_flag"]
        & ~df["invalid_livingarea_flag"]
        & ~df["invalid_daysonmarket_flag"]
        & ~df["invalid_bedrooms_flag"]
        & ~df["invalid_bathrooms_flag"]
    ].copy()

    rows_after = len(clean_df)
    cols_after = clean_df.shape[1]

    # --------------------------------------------------
    # 9. Reporting summaries
    # --------------------------------------------------
    print("\nRow count summary:")
    print(f"Rows before cleaning: {rows_before}")
    print(f"Rows after removing invalid numeric rows: {rows_after}")

    print("\nDate consistency flag counts:")
    for col in [
        "listing_after_close_flag",
        "purchase_after_close_flag",
        "negative_timeline_flag",
    ]:
        print(f"{col}: {int(df[col].sum())}")

    print("\nGeographic data quality summary:")
    for col in [
        "missing_coordinate_flag",
        "zero_coordinate_flag",
        "positive_longitude_flag",
        "implausible_coordinate_flag",
    ]:
        print(f"{col}: {int(df[col].sum())}")

    print("\nInvalid numeric flag counts:")
    for col in [
        "invalid_closeprice_flag",
        "invalid_livingarea_flag",
        "invalid_daysonmarket_flag",
        "invalid_bedrooms_flag",
        "invalid_bathrooms_flag",
    ]:
        print(f"{col}: {int(df[col].sum())}")

    print("\nSelected cleaned dtypes:")
    dtype_check_cols = [c for c in DATE_COLS + NUMERIC_COLS if c in clean_df.columns]
    print(clean_df[dtype_check_cols].dtypes)

    # --------------------------------------------------
    # 10. Save outputs
    # --------------------------------------------------
    clean_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    summary_rows = [
        {"metric": "rows_before_cleaning", "value": rows_before},
        {"metric": "rows_after_invalid_numeric_filter", "value": rows_after},
        {"metric": "columns_before_cleaning", "value": cols_before},
        {"metric": "columns_after_cleaning", "value": cols_after},
    ]
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_file.parent / f"{dataset_name}_week4_rowcount_summary.csv",
                      index=False, encoding="utf-8-sig")

    flag_summary = pd.DataFrame({
        "flag_name": flag_cols,
        "flag_count": [int(df[col].sum()) for col in flag_cols]
    })
    flag_summary.to_csv(output_file.parent / f"{dataset_name}_week4_flag_summary.csv",
                        index=False, encoding="utf-8-sig")

    dtype_summary = pd.DataFrame({
        "column": clean_df.columns,
        "dtype": clean_df.dtypes.astype(str).values
    })
    dtype_summary.to_csv(output_file.parent / f"{dataset_name}_week4_dtype_summary.csv",
                         index=False, encoding="utf-8-sig")

    print(f"\nSaved cleaned dataset to: {output_file}")
    print(f"Saved summaries in: {output_file.parent}")


def main():
    clean_dataset(
        input_file=LISTING_FILE,
        output_file=BASE_DIR / "Listing" / "CRMLSListing_week4_cleaned.csv",
        dataset_name="Listing"
    )

    clean_dataset(
        input_file=SOLD_FILE,
        output_file=BASE_DIR / "Sold" / "CRMLSSold_week4_cleaned.csv",
        dataset_name="Sold"
    )


if __name__ == "__main__":
    main()