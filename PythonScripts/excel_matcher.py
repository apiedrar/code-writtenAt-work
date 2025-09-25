#!/usr/bin/env python3
"""
Excel Column Matcher Script

This script efficiently matches values between two columns in an Excel file
and outputs the results to a new Excel file.

Usage:
    python excel_matcher.py input.xlsx output.xlsx --col1 RequestID --col2 ID

Requirements:
    pip install pandas openpyxl
"""

import argparse
import pandas as pd
import sys
from pathlib import Path


def match_columns(input_file, output_file, col1_name, col2_name, sheet_name=None):
    """
    Match values between two columns in an Excel file.

    Args:
        input_file (str): Path to input Excel file
        output_file (str): Path to output Excel file
        col1_name (str): Name of the first column (source column)
        col2_name (str): Name of the second column (column to check against)
        sheet_name (str): Sheet name to read (None for first sheet)
    """

    print(f"Reading Excel file: {input_file}")

    try:
        # Read the Excel file
        if sheet_name:
            df = pd.read_excel(input_file, sheet_name=sheet_name)
        else:
            df = pd.read_excel(input_file)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)

    # Check if columns exist
    if col1_name not in df.columns:
        print(f"Error: Column '{col1_name}' not found in the Excel file.")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    if col2_name not in df.columns:
        print(f"Error: Column '{col2_name}' not found in the Excel file.")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    print(f"Processing columns: '{col1_name}' and '{col2_name}'")
    print(f"Total rows in {col1_name}: {len(df[col1_name])}")
    print(f"Total rows in {col2_name}: {len(df[col2_name])}")

    # Handle blank/null values gracefully
    # Remove NaN, None, and empty string values from both columns
    col1_clean = df[col1_name].dropna()
    col1_clean = col1_clean[col1_clean != ""]
    col1_clean = col1_clean.astype(str).str.strip()
    col1_clean = col1_clean[col1_clean != ""]

    col2_clean = df[col2_name].dropna()
    col2_clean = col2_clean[col2_clean != ""]
    col2_clean = col2_clean.astype(str).str.strip()
    col2_clean = col2_clean[col2_clean != ""]

    print(f"Non-blank rows in {col1_name}: {len(col1_clean)}")
    print(f"Non-blank rows in {col2_name}: {len(col2_clean)}")

    # Create a set from col1 for faster lookup
    col1_set = set(col1_clean.values)
    print(f"Unique values in {col1_name}: {len(col1_set)}")

    # Find matches - check which values in col2 exist in col1
    print("Finding matches...")
    matches_mask = col2_clean.isin(col1_set)
    matched_values = col2_clean[matches_mask]

    print(f"Number of matches found: {len(matched_values)}")

    # Create results DataFrame
    results = []

    # Add all original data with match indicators
    df_result = df.copy()

    # Create a match indicator column for col2
    df_result[f"{col2_name}_has_match"] = False
    df_result[f"{col2_name}_match_count"] = 0

    # Process each row in col2
    for idx, value in df[col2_name].items():
        if pd.notna(value) and str(value).strip() != "":
            clean_value = str(value).strip()
            if clean_value in col1_set:
                df_result.loc[idx, f"{col2_name}_has_match"] = True
                df_result.loc[idx, f"{col2_name}_match_count"] = 1

    # Create a summary sheet with just the matches
    matched_rows = df_result[df_result[f"{col2_name}_has_match"] == True]

    # Create unique matches summary
    unique_matches = pd.DataFrame(
        {
            "Matched_Value": matched_values.unique(),
            "Source_Column": col2_name,
            "Found_In_Column": col1_name,
        }
    )

    print(f"Unique matched values: {len(unique_matches)}")

    # Save results to Excel with multiple sheets
    print(f"Saving results to: {output_file}")

    try:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Sheet 1: All data with match indicators
            df_result.to_excel(writer, sheet_name="All_Data_With_Matches", index=False)

            # Sheet 2: Only matched rows
            if len(matched_rows) > 0:
                matched_rows.to_excel(
                    writer, sheet_name="Matched_Rows_Only", index=False
                )

            # Sheet 3: Summary of unique matches
            if len(unique_matches) > 0:
                unique_matches.to_excel(
                    writer, sheet_name="Unique_Matches_Summary", index=False
                )

            # Sheet 4: Statistics summary
            stats_df = pd.DataFrame(
                {
                    "Metric": [
                        f"Total rows in {col1_name}",
                        f"Non-blank rows in {col1_name}",
                        f"Unique values in {col1_name}",
                        f"Total rows in {col2_name}",
                        f"Non-blank rows in {col2_name}",
                        f"Matched values in {col2_name}",
                        f"Unique matched values",
                        "Match percentage",
                    ],
                    "Value": [
                        len(df[col1_name]),
                        len(col1_clean),
                        len(col1_set),
                        len(df[col2_name]),
                        len(col2_clean),
                        len(matched_values),
                        len(unique_matches),
                        (
                            f"{(len(matched_values) / len(col2_clean) * 100):.2f}%"
                            if len(col2_clean) > 0
                            else "0%"
                        ),
                    ],
                }
            )
            stats_df.to_excel(writer, sheet_name="Statistics", index=False)

        print("✅ Results saved successfully!")
        print(f"📊 Match summary:")
        print(f"   - Total matches: {len(matched_values)}")
        print(f"   - Unique matches: {len(unique_matches)}")
        print(
            f"   - Match rate: {(len(matched_values) / len(col2_clean) * 100):.2f}%"
            if len(col2_clean) > 0
            else "   - Match rate: 0%"
        )

    except Exception as e:
        print(f"Error saving Excel file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Match values between two columns in a file (CSV or Excel)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python column_matcher.py data.csv results.xlsx --col1 RequestID --col2 ID
  python column_matcher.py data.xlsx results.xlsx --col1 RequestID --col2 ID
  python column_matcher.py input.csv output.xlsx --col1 "Request ID" --col2 "Customer ID" --delimiter ";"
  python column_matcher.py input.xlsx output.xlsx --col1 "Request ID" --col2 "Customer ID" --sheet "Sheet1"
        """,
    )

    parser.add_argument("input_file", help="Path to input file (CSV or Excel)")
    parser.add_argument("output_file", help="Path to output Excel file")
    parser.add_argument(
        "--col1", required=True, help="Name of the source column (e.g., RequestID)"
    )
    parser.add_argument(
        "--col2",
        required=True,
        help="Name of the column to check for matches (e.g., ID)",
    )
    parser.add_argument(
        "--sheet", help="Sheet name to read (Excel files only, optional)"
    )
    parser.add_argument(
        "--delimiter", default=",", help="CSV delimiter (default: comma)"
    )

    args = parser.parse_args()

    # Validate input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' does not exist.")
        sys.exit(1)

    # Ensure output file has .xlsx extension
    output_path = Path(args.output_file)
    if output_path.suffix.lower() != ".xlsx":
        args.output_file = str(output_path.with_suffix(".xlsx"))
        print(f"Output file extension changed to .xlsx: {args.output_file}")

    # Create output directory if it doesn't exist
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Run the matching process
    match_columns(
        input_file=args.input_file,
        output_file=args.output_file,
        col1_name=args.col1,
        col2_name=args.col2,
        sheet_name=args.sheet,
        csv_delimiter=args.delimiter,
    )


if __name__ == "__main__":
    main()
