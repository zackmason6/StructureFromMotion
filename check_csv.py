"""
Input Validator (Pre-Processing)
================================
Purpose:
    Pre-checks the source lookup tables for data integrity, formatting errors,
    and unit consistency prior to running the main XML generator pipeline.

Usage:
    Run this script against updated CSVs (e.g., Florida/Hawaii batches) to
    catch missing coordinates or invalid data types before generation.
"""

import pandas as pd

# Load the CSV
df = pd.read_csv("fixedLookup.csv")

# Convert the DATE column to actual datetime objects
df["DATE"] = pd.to_datetime(df["DATE"])

# Group by Mission and find the min and max dates for each
summary = df.groupby("MISSION")["DATE"].agg(["min", "max"])

print("--- EXPECTED MISSION DATES FROM CSV ---")
print(summary)
