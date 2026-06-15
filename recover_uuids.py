"""
UUID Recovery Utility
=====================
Purpose:
    Utility script designed to parse existing catalog records or legacy datasets
    to rebuild the UUID lookup table.

Usage:
    To be run only in the event of local file corruption, catalog mismatch,
    or when migrating historical accessions into the current tracking system to
    prevent the creation of duplicate records in the NOAA Catalog.
"""

import os
import re
import csv

old_master_csv = "uuidLookup.csv"  # Your existing master list
new_master_csv = "UPDATED_MASTER_uuidLookup.csv"

print("Scanning your local folders for true UUIDs...")

# 1. Get all your perfect local data
local_data = {}
for root, dirs, files in os.walk("."):
    for filename in files:
        if filename.endswith(".xml"):
            filepath = os.path.join(root, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                match = re.search(r'uuid="([^"]+)"', content)
                if match:
                    real_uuid = match.group(1)
                    tar_filename = filename.replace(".xml", ".tar")
                    local_data[tar_filename] = real_uuid

print(f"Found {len(local_data)} true files in your folders.")

# 2. Read the old CSV and keep ONLY the historical stuff
historical_rows = []
headers = []

with open(old_master_csv, "r", encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)
    headers = next(reader, ["Filename", "UUID"])  # Grab headers
    for row in reader:
        if len(row) >= 2:
            filename = row[0]
            # If the filename from the CSV is NOT in your local folders, it's historical. Keep it!
            if filename not in local_data:
                historical_rows.append(row)

# 3. Write the new hybrid file
with open(new_master_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)

    # Write the untouched historical data first
    writer.writerows(historical_rows)

    # Write your clean, deduplicated data at the bottom
    for filename, real_uuid in local_data.items():
        writer.writerow([filename, real_uuid])

print(f"Done! Saved to {new_master_csv}.")
print(
    f"Kept {len(historical_rows)} historical records and updated your {len(local_data)} perfect records."
)
