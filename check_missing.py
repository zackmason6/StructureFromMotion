"""
Completeness Auditor
====================
Purpose:
    Cross-references the final generated XML directory against the original
    source inputs (.tar packages / CSV tables) to verify parity.

Usage:
    Identifies any dropped sites or failed generations within a batch run
    to ensure 100% data completeness before catalog submission.
"""

import pandas as pd

# --- CHANGE THIS LINE TO TEST DIFFERENT CRUISES ---
answer_key_file = "NCRMP_SFM_StRS_2024_SE2406_MHI_metadata.csv"
receipt_file = "uuidLookup.csv"

print(f"--- Checking {answer_key_file} ---")

try:
    # Load the Team Lead's CSV and your UUID list
    expected_df = pd.read_csv(answer_key_file)
    # Assuming uuidLookup has no headers, just filename and uuid
    actual_df = pd.read_csv(receipt_file, header=None, names=["filename", "uuid"])

    # Grab all the sites your Team Lead expects
    expected_sites = expected_df["SITE"].dropna().unique()

    missing_sites = []
    found_count = 0

    for site in expected_sites:
        # If the site ID exists anywhere in your completed filename list, it's a match
        if actual_df["filename"].str.contains(str(site), regex=False).any():
            found_count += 1
        else:
            missing_sites.append(site)

    print(f"Total Sites Expected: {len(expected_sites)}")
    print(f"Total Sites Found: {found_count}")
    print(f"Total Sites MISSING: {len(missing_sites)}")

    if len(missing_sites) > 0:
        print("\nHere are the exact sites you are missing:")
        for missing in missing_sites:
            print(f"- {missing}")
    else:
        print("\nSUCCESS! You have generated 100% of the files for this cruise.")

except FileNotFoundError:
    print(f"Error: Make sure {answer_key_file} and {receipt_file} are in this folder!")
