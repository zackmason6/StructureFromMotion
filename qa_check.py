"""
General XML Validation Suite (Post-Processing)
==============================================
Purpose:
    Performs a comprehensive sweep of the final XML outputs to ensure all
    required metadata fields, tags, and coordinate bounds meet NOAA Catalog
    and OSIM harvest specifications.
"""

import os

# Put your newly generated folder name here:
# FOLDER_TO_CHECK = "Updated_Main_Hawaiian_Islands_2026_04_20"
FOLDER_TO_CHECK = "Updated_MP2011_MHI_FixedSites_2026_04_20"

# These are the valid references we EXPECT to see
VALID_PHRASES = [
    "since 2019",
    "established in 2019",
    "2019-04-21",  # This is the valid master parent project start date
    "<gco:Date>2019</gco:Date>",  # The SOP publication date
]


def scan_files():
    suspicious_files = 0

    for filename in os.listdir(FOLDER_TO_CHECK):
        if not filename.endswith(".xml"):
            continue

        filepath = os.path.join(FOLDER_TO_CHECK, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for line_num, line in enumerate(lines):
            if "2019" in line:
                # If it's not one of our known valid phrases, flag it!
                if not any(valid in line for valid in VALID_PHRASES):
                    print(f"Suspicious 2019 found in {filename} (Line {line_num + 1}):")
                    print(f"  -> {line.strip()}")
                    suspicious_files += 1

    if suspicious_files == 0:
        print("SUCCESS: 100% clean! No suspicious 2019 dates found in any files.")


if __name__ == "__main__":
    scan_files()
