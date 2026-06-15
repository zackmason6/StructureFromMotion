"""
Legacy Diagnostic & Audit Tool
==============================
Purpose:
    Initial diagnostic script used to map raw data structures and identify
    deep-seated anomalies (e.g., column shifts, swapped date/time fields)
    within historical accessions before they enter the processing pipeline.

Note:
    Used primarily for scoping new/legacy data batches (like ERDDAP integrations)
    rather than day-to-day SFM XML generation.
"""

import pandas as pd

# Check if the coordinates are in the right columns
df = pd.read_csv("strsLookup.csv")
sample = df[df["REGION"] == "MARI"].iloc[0]

print(f"--- DATA AUDIT ---")
print(f"Checking Site: {sample['SITE']}")
print(f"Is Latitude a number? {str(sample['LATITUDE']).replace('.','').isdigit()}")
print(f"Is Longitude a number? {str(sample['LONGITUDE']).replace('.','').isdigit()}")
print(f"Is Survey Size '3x20m' or similar? {'x' in str(sample['SURVEY_SIZE'])}")
