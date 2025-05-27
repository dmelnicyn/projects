import pandas as pd
from google.cloud import storage
import os

# ---- Step 1: Load Raw Data from VM ----
file_path = "/home/dmelnitsyn/data-engineering-zoomcamp/projects/my-first-de-project/GIS/group_length.csv"  # Update this path
df = pd.read_csv(file_path, delimiter=";")

# ---- Step 2: Transform & Clean ----
# Rename columns
df.rename(columns={"ZONE_min": "functional_area", "function": "road_type", "None": "length"}, inplace=True)

# Replace values in road_type
df["road_type"] = df["road_type"].replace({
    "Local Access Road": "Local Street",
    "Restricted Local Access Road": "Motor vehicle-free",
    None: "Unclassified",
    "Local Road": "Local Street"
})

# Merge rows with the same functional_area and road_type (sum the length)
df = df.groupby(["functional_area", "road_type"], as_index=False).agg({"length": "sum"})

# ---- Step 3: Save Cleaned Data Locally ----
cleaned_file_path = "/home/dmelnitsyn/data-engineering-zoomcamp/projects/my-first-de-project/tfl-cycles/seeds/cleaned_group_length.csv"  # Update this path
df.to_csv(cleaned_file_path, index=False)

# ---- Step 4: Upload to GCS ----
bucket_name = "tfl-cycles-bucket-453703"  # Replace with your actual GCS bucket
destination_blob_name = "cleaned_group_length.csv"

"""# Initialize GCS client
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

# Upload file to GCS
blob.upload_from_filename(cleaned_file_path)

print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
"""