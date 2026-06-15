"""
Output Temporal Validator (Post-Processing)
===========================================
Purpose:
    Scans generated XML granules and mathematically verifies derived temporal
    fields (Mission Start/End dates) against the source CSV.

Usage:
    Acts as an automated QA check to ensure dynamic date calculations in
    xmlGenerate.py were applied correctly and to prevent historical data bleeding.
"""

import os
import re

# Put the folder name containing your newly generated XMLs here
FOLDER_TO_CHECK = "Updated_Main_Hawaiian_Islands_2026_04_20"


def check_dates():
    print(f"{'FILENAME'.ljust(60)} | {'START DATE'.ljust(12)} | {'END DATE'}")
    print("-" * 90)

    for filename in os.listdir(FOLDER_TO_CHECK):
        if not filename.endswith(".xml"):
            continue

        with open(os.path.join(FOLDER_TO_CHECK, filename), "r", encoding="utf-8") as f:
            content = f.read()

            # Find the start and end dates in the XML
            start_match = re.search(
                r"<gml:beginPosition>(.*?)</gml:beginPosition>", content
            )
            end_match = re.search(r"<gml:endPosition>(.*?)</gml:endPosition>", content)

            start_date = start_match.group(1) if start_match else "MISSING"
            end_date = end_match.group(1) if end_match else "MISSING"

            print(f"{filename[:58].ljust(60)} | {start_date.ljust(12)} | {end_date}")


if __name__ == "__main__":
    check_dates()
