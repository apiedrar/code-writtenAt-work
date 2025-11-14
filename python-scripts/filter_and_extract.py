#!/usr/bin/env python3
"""
Excel Filter and Copy Script

This script reads an Excel file, applies filters to specified columns,
and saves the filtered data to a new Excel file.

Usage examples:
    python excel_filter.py input.xlsx output.xlsx --filter "Date > '2025-06-30' and Date < '2025-08-01'"
    python excel_filter.py input.xlsx output.xlsx --filter "Age >= 18" --sheet "Sheet1"
    python excel_filter.py input.xlsx output.xlsx --filter "Status == 'Active' and Score > 80"
"""

import pandas as pd
import argparse
import sys
from datetime import datetime


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Filter and copy Excel data based on column conditions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.xlsx output.xlsx --filter "Date > '2025-06-30'"
  %(prog)s input.xlsx output.xlsx --filter "Age >= 18 and Status == 'Active'"
  %(prog)s input.xlsx output.xlsx --filter "Price > 100" --sheet "Data"
  
Filter syntax:
  - Use column names exactly as they appear in the Excel file
  - For dates, use quotes: "Date > '2025-01-01'"
  - For strings, use quotes: "Status == 'Active'"
  - For numbers: "Age >= 18"
  - Combine with 'and', 'or': "Age >= 18 and Status == 'Active'"
  - Operators: ==, !=, >, <, >=, <=
        """,
    )

    parser.add_argument("input_file", help="Input Excel file path")
    parser.add_argument("output_file", help="Output Excel file path")
    parser.add_argument(
        "--filter",
        "-f",
        required=True,
        help="Filter condition (e.g., \"Date > '2025-06-30'\")",
    )
    parser.add_argument(
        "--sheet", "-s", default=0, help="Sheet name or index (default: first sheet)"
    )
    parser.add_argument(
        "--output-sheet",
        "-os",
        default="Sheet1",
        help="Output sheet name (default: Sheet1)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Print verbose output"
    )

    return parser.parse_args()


def load_excel_data(file_path, sheet_name):
    """Load data from Excel file."""
    try:
        if isinstance(sheet_name, str) and sheet_name.isdigit():
            sheet_name = int(sheet_name)

        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)


def apply_filter(df, filter_condition, verbose=False):
    """Apply filter condition to DataFrame."""
    try:
        if verbose:
            print(f"Original data shape: {df.shape}")
            print(f"Applying filter: {filter_condition}")

        # Convert date-like strings to datetime if they look like dates
        for col in df.columns:
            if df[col].dtype == "object":
                # Try to convert to datetime if it looks like a date
                try:
                    sample = (
                        df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    )
                    if sample and isinstance(sample, str):
                        if any(char in str(sample) for char in ["-", "/"]):
                            df[col] = pd.to_datetime(df[col], errors="ignore")
                except:
                    pass

        # Apply the filter
        filtered_df = df.query(filter_condition)

        if verbose:
            print(f"Filtered data shape: {filtered_df.shape}")
            print(f"Rows removed: {len(df) - len(filtered_df)}")

        return filtered_df

    except Exception as e:
        print(f"Error applying filter: {e}")
        print("\nTips for filter syntax:")
        print("- Use column names exactly as they appear in Excel")
        print("- For dates: Date > '2025-06-30'")
        print("- For strings: Status == 'Active'")
        print("- For numbers: Age >= 18")
        print("- Available columns:", list(df.columns))
        sys.exit(1)


def save_excel_data(df, file_path, sheet_name, verbose=False):
    """Save DataFrame to Excel file."""
    try:
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        if verbose:
            print(f"Successfully saved {len(df)} rows to '{file_path}'")
            print(f"Sheet name: '{sheet_name}'")

    except Exception as e:
        print(f"Error saving Excel file: {e}")
        sys.exit(1)


def main():
    """Main function."""
    args = parse_arguments()

    if args.verbose:
        print(f"Input file: {args.input_file}")
        print(f"Output file: {args.output_file}")
        print(f"Sheet: {args.sheet}")
        print(f"Filter: {args.filter}")
        print("-" * 50)

    # Load data
    df = load_excel_data(args.input_file, args.sheet)

    if args.verbose:
        print(f"Loaded data from '{args.input_file}'")
        print(f"Columns: {list(df.columns)}")

    # Apply filter
    filtered_df = apply_filter(df, args.filter, args.verbose)

    # Check if any data remains after filtering
    if len(filtered_df) == 0:
        print("Warning: No rows match the filter condition.")
        print("The output file will be created but will be empty (except headers).")

    # Save filtered data
    save_excel_data(filtered_df, args.output_file, args.output_sheet, args.verbose)

    print(f"Successfully filtered and copied data to '{args.output_file}'")


if __name__ == "__main__":
    main()
