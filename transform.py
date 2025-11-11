import pandas as pd
from logger_config import setup_logger

logger = setup_logger()

def transform_data(df):
    logger.info("Starting data transformation...")

    # Renaming columns 
    df.rename(columns={
        "id": "ProductID",
        "title": "ProductName",
        "price": "UnitPrice",
        "quantity": "Quantity",
        "total": "TotalPrice",
        "discountPercentage": "DiscountPercent",
        "discountedPrice": "DiscountedPrice",
        "userId": "UserID",
        "id.1": "CartID"
    }, inplace=True)

    # Handling missing or invalid data
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["DiscountedPrice"] = pd.to_numeric(df["DiscountedPrice"], errors="coerce").fillna(0)
    df["DiscountPercent"] = pd.to_numeric(df["DiscountPercent"], errors="coerce").fillna(0)

    # Remove rows with zero or negative prices/quantities
    df = df[(df["UnitPrice"] > 0) & (df["Quantity"] > 0)]

    # Derived metrics
    df["NetRevenue"] = df["Quantity"] * df["DiscountedPrice"]
    df["DiscountAmount"] = (df["UnitPrice"] - df["DiscountedPrice"]) * df["Quantity"]
    df["DiscountPercent_Recalc"] = ((df["UnitPrice"] - df["DiscountedPrice"]) / df["UnitPrice"]) * 100

    # Categorize based on Net Revenue
    df["SaleCategory"] = pd.cut(
        df["NetRevenue"],
        bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
        labels=["Very Low", "Low", "Medium", "High", "Very High", "Ultra High"]
    )

    # Feature Engineering
    df["IsDiscounted"] = df["DiscountPercent"] > 0
    df["RevenuePerUnit"] = df["NetRevenue"] / df["Quantity"]
    df["DiscountEfficiency"] = (df["DiscountAmount"] / df["NetRevenue"]).round(2)

    # Date enrichment (if timestamp or order date exists)
    if "date" in df.columns:
        df["OrderDate"] = pd.to_datetime(df["date"], errors="coerce")
        df["OrderMonth"] = df["OrderDate"].dt.month
        df["OrderYear"] = df["OrderDate"].dt.year
        df["OrderWeekday"] = df["OrderDate"].dt.day_name()
        df["IsWeekend"] = df["OrderDate"].dt.weekday >= 5

    # Flag high performing products
    df["HighPerformer"] = (df["NetRevenue"] > 1000) & (df["DiscountPercent"] < 20)

    # Add profitability scoring based on Discounted Price
    df["ProfitMargin"] = (df["DiscountedPrice"] * 0.8) - (df["DiscountedPrice"] * 0.6)
    df["ProfitCategory"] = pd.cut(
        df["ProfitMargin"],
        bins=[-100, 0, 50, 100, 500, float('inf')],
        labels=["Loss", "Low", "Medium", "High", "Very High"]
    )

    # Drop duplicates (if any)
    df.drop_duplicates(subset=["CartID", "ProductID"], inplace=True)

    # Final cleanup & logging
    df.reset_index(drop=True, inplace=True)
    logger.info(f"✅ Transformation completed. Total records processed: {len(df)}")

    return df
