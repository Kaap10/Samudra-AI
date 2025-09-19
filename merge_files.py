import pandas as pd

print("--- Starting Data Merging Process ---")

# --- Step 1: Load your separate CSV files ---
try:
    ocean_df = pd.read_csv('testing/oceanography_data.csv')
    bio_df = pd.read_csv('testing/biodiversity_data.csv')
    fisheries_df = pd.read_csv('testing/fisheries_data.csv')
    print("=> Successfully loaded 3 separate CSV files.")

except FileNotFoundError as e:
    print(f"Error: {e}. Make sure all your CSV files are in the same folder as this script.")
    exit()

# --- Step 2: Prepare for Merging ---
print("=> Standardizing common columns for the merge...")
ocean_df['date'] = pd.to_datetime(ocean_df['reading_date'], errors='coerce').dt.date
bio_df['date'] = pd.to_datetime(bio_df['survey_date'], errors='coerce').dt.date
fisheries_df['date'] = pd.to_datetime(fisheries_df['catch_date'], errors='coerce').dt.date

ocean_df['location'] = ocean_df['location'].str.lower()
bio_df['location'] = bio_df['region'].str.lower()
fisheries_df['location'] = fisheries_df['area'].str.lower()

# --- Step 3: Merge the DataFrames ---
print("=> Merging data based on date and location...")
merged_df = pd.merge(ocean_df, bio_df, on=['date', 'location'], how='outer')

# --- NEW FIX: Unify the duplicate oxygen column ---
# If the first oxygen column ('_x') is empty, use the value from the second one ('_y')
merged_df['dissolved_oxygen_mg_l'] = merged_df['dissolved_oxygen_mg_l_x'].fillna(merged_df['dissolved_oxygen_mg_l_y'])
# Now, drop the old, separate '_x' and '_y' columns
merged_df.drop(columns=['dissolved_oxygen_mg_l_x', 'dissolved_oxygen_mg_l_y'], inplace=True)
# --- END OF FIX ---

final_merged_df = pd.merge(merged_df, fisheries_df, on=['date', 'location'], how='outer')

# --- Step 4: Save the final messy CSV ---
final_merged_df.to_csv('merged_raw_data.csv', index=False)

print("\n--- Merging Finished Successfully! ---")
print("A new file named 'merged_raw_data.csv' has been created. This is the messy file your main pipeline will clean.")