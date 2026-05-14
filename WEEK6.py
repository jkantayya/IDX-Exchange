import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

LISTING_FILE = BASE_DIR / "listing" / "CRMLSListing_week4_cleaned.csv"
SOLD_FILE = BASE_DIR / "sold" / "CRMLSSold_week4_cleaned.csv"


def create_features(df, dataset_name):
    print("=" * 80)
    print(f"Creating features for {dataset_name}")

    # -------------------------------
    # 1. Price per square foot
    # -------------------------------
    if "ClosePrice" in df.columns and "LivingArea" in df.columns:
        df["price_per_sqft"] = df["ClosePrice"] / df["LivingArea"]

    if "ListPrice" in df.columns and "LivingArea" in df.columns:
        df["list_price_per_sqft"] = df["ListPrice"] / df["LivingArea"]

    # -------------------------------
    # 2. Price ratio (sold vs list)
    # -------------------------------
    if "ClosePrice" in df.columns and "ListPrice" in df.columns:
        df["sale_to_list_ratio"] = df["ClosePrice"] / df["ListPrice"]

    # -------------------------------
    # 3. Time features
    # -------------------------------
    if {"ListingContractDate", "PurchaseContractDate"}.issubset(df.columns):
        df["days_to_contract"] = (
            df["PurchaseContractDate"] - df["ListingContractDate"]
        ).dt.days

    if {"PurchaseContractDate", "CloseDate"}.issubset(df.columns):
        df["days_to_close"] = (
            df["CloseDate"] - df["PurchaseContractDate"]
        ).dt.days

    if {"ListingContractDate", "CloseDate"}.issubset(df.columns):
        df["total_days_to_close"] = (
            df["CloseDate"] - df["ListingContractDate"]
        ).dt.days

    # -------------------------------
    # 4. Price buckets
    # -------------------------------
    if "ClosePrice" in df.columns:
        df["price_bucket"] = pd.cut(
            df["ClosePrice"],
            bins=[0, 250000, 500000, 750000, 1000000, 2000000, 10000000],
            labels=[
                "<250k",
                "250k-500k",
                "500k-750k",
                "750k-1M",
                "1M-2M",
                "2M+",
            ],
        )

    # -------------------------------
    # 5. Size buckets
    # -------------------------------
    if "LivingArea" in df.columns:
        df["size_bucket"] = pd.cut(
            df["LivingArea"],
            bins=[0, 1000, 2000, 3000, 5000, 20000],
            labels=[
                "<1000",
                "1000-2000",
                "2000-3000",
                "3000-5000",
                "5000+",
            ],
        )

    print("Features created successfully")
    return df


def process_dataset(input_file, output_file, name):
    print(f"\nLoading {name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig")

    # Convert date columns again (safe)
    date_cols = [
        "CloseDate",
        "PurchaseContractDate",
        "ListingContractDate",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = create_features(df, name)

    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Saved to {output_file}")


def main():
    process_dataset(
        LISTING_FILE,
        BASE_DIR / "listing" / "CRMLSListing_week6_features.csv",
        "Listing",
    )

    process_dataset(
        SOLD_FILE,
        BASE_DIR / "sold" / "CRMLSSold_week6_features.csv",
        "Sold",
    )


if __name__ == "__main__":
    main()