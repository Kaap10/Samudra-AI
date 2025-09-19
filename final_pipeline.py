# """
# run_processing_pipeline.py

# Data Cleaning + PostgreSQL Loader

# Pipeline steps:
# 1. Read raw merged CSV
# 2. Unify and clean date & location columns
# 3. Convert numeric values & impute missing
# 4. Save cleaned data to CSV
# 5. Load cleaned data into PostgreSQL (visible in pgAdmin)
# """

# import pandas as pd
# from sqlalchemy import create_engine

# # --- Configuration ---
# RAW_CSV = "merged_raw_data.csv"          # Input file (messy merged data)
# CLEAN_CSV = "clean_data_table.csv"       # Output file (cleaned data)

# # PostgreSQL connection details
# DB_USER = "postgres"
# DB_PASS = "123"   # <-- change this
# DB_HOST = "localhost"
# DB_PORT = "5432"
# DB_NAME = "samudra_ai"
# DB_TABLE = "clean_data"


# def run_cleaning_pipeline():
#     """Run cleaning pipeline and load results into PostgreSQL."""
#     print("\n--- Starting Data Cleaning Pipeline ---")

#     try:
#         # --------------------------------------------------
#         # Step 1: Load Raw Data
#         # --------------------------------------------------
#         df = pd.read_csv(RAW_CSV)
#         print(f"[INFO] Loaded '{RAW_CSV}' with {len(df)} rows.")

#         # --------------------------------------------------
#         # Step 2: Unify Date Columns
#         # --------------------------------------------------
#         date_cols = ["reading_date", "survey_date", "catch_date"]
#         date_df = df[date_cols].copy()

#         for col in date_cols:
#             date_df[col] = pd.to_datetime(date_df[col], errors="coerce")

#         df["timestamp"] = date_df.bfill(axis=1).iloc[:, 0]
#         df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
#         df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")

#         # --------------------------------------------------
#         # Step 3: Unify Location Columns
#         # --------------------------------------------------
#         df["location"] = (
#             df.get("location")
#             .fillna(df.get("region"))
#             .fillna(df.get("area"))
#             .str.lower()
#         )

#         # --------------------------------------------------
#         # Step 4: Clean Numeric Columns
#         # --------------------------------------------------
#         numeric_cols = [
#             "temperature_celsius",
#             "salinity_psu",
#             "ph_level",
#             "dissolved_oxygen_mg_l",
#             "catch_kg",
#         ]
#         for col in numeric_cols:
#             if col in df.columns:
#                 df[col] = pd.to_numeric(df[col], errors="coerce")

#         # --------------------------------------------------
#         # Step 5: Imputation
#         # --------------------------------------------------
#         for col in numeric_cols:
#             if col in df.columns:
#                 df[col] = df.groupby("location")[col].transform(
#                     lambda x: x.fillna(round(x.mean(), 2))
#                 )

#         # --------------------------------------------------
#         # Step 6: Finalize Clean Data
#         # --------------------------------------------------
#         final_cols = [
#             "timestamp",
#             "location",
#             "temperature_celsius",
#             "salinity_psu",
#             "ph_level",
#             "dissolved_oxygen_mg_l",
#             "dominant_species",
#             "catch_kg",
#         ]
#         final_df = df[[col for col in final_cols if col in df.columns]].copy()
#         final_df.dropna(subset=["timestamp", "location"], inplace=True)

#         # Save to CSV
#         final_df.to_csv(CLEAN_CSV, index=False)
#         print(f"[SUCCESS] Saved {len(final_df)} cleaned rows to '{CLEAN_CSV}'.")

#         # --------------------------------------------------
#         # Step 7: Insert into PostgreSQL
#         # --------------------------------------------------
#         print("[INFO] Inserting cleaned data into PostgreSQL...")

#         engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
#         final_df.to_sql(DB_TABLE, engine, if_exists="replace", index=False)

#         print(f"[SUCCESS] Loaded {len(final_df)} rows into table '{DB_TABLE}' (DB: {DB_NAME}).")

#     except FileNotFoundError:
#         print(f"[ERROR] Input file '{RAW_CSV}' not found.")
#     except Exception as e:
#         print(f"[ERROR] Unexpected error: {e}")


# if __name__ == "__main__":
#     run_cleaning_pipeline()

import pandas as pd
from sqlalchemy import create_engine

RAW_CSV = "merged_raw_data.csv"
CLEAN_CSV = "clean_data_table.csv"

DB_USER = "postgres"
DB_PASS = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "samudra_ai"
DB_TABLE = "clean_data"


def run_cleaning_pipeline():
    print("\n--- Starting Data Cleaning Pipeline ---")

    try:
        df = pd.read_csv(RAW_CSV)
        print(f"[INFO] Loaded '{RAW_CSV}' with {len(df)} rows.")

        # --- Date Columns ---
        date_cols = ["reading_date", "survey_date", "catch_date"]
        for col in date_cols:
            if col in df.columns:
                if col == "catch_date":
                    df[col] = df[col].astype(str).str.extract(r"(\d{4})")[0]
                    df[col] = pd.to_datetime(df[col] + "-01-01", errors="coerce")
                else:
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        df["timestamp"] = df[date_cols].bfill(axis=1).iloc[:, 0]
        df = df[df["timestamp"].notna()].copy()
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")

        # --- Location ---
        df["location"] = (
            df.get("location")
            .fillna(df.get("region"))
            .fillna(df.get("area"))
            .str.lower()
        )

        # --- Numeric Cleaning ---
        numeric_cols = [
            "temperature_celsius",
            "salinity_psu",
            "ph_level",
            "dissolved_oxygen_mg_l",
            "catch_kg",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Imputation ---
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df.groupby("location")[col].transform(
                    lambda x: x.fillna(round(x.mean(), 2))
                )

        # --- Final Dataset ---
        final_cols = [
            "timestamp",
            "location",
            "temperature_celsius",
            "salinity_psu",
            "ph_level",
            "dissolved_oxygen_mg_l",
            "dominant_species",
            "catch_kg",
        ]
        final_df = df[[col for col in final_cols if col in df.columns]].copy()
        final_df.dropna(subset=["timestamp", "location"], inplace=True)

        final_df.to_csv(CLEAN_CSV, index=False)
        print(f"[SUCCESS] Saved {len(final_df)} cleaned rows to '{CLEAN_CSV}'.")

        # --- PostgreSQL Load ---
        print("[INFO] Inserting cleaned data into PostgreSQL...")
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        final_df.to_sql(DB_TABLE, engine, if_exists="replace", index=False)
        print(f"[SUCCESS] Loaded {len(final_df)} rows into table '{DB_TABLE}' (DB: {DB_NAME}).")

    except FileNotFoundError:
        print(f"[ERROR] Input file '{RAW_CSV}' not found.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


if __name__ == "__main__":
    run_cleaning_pipeline()
