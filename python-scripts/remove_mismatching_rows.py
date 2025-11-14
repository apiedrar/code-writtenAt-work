#!/usr/bin/env python3
"""
Script to remove rows from the first document that don't match in the second document.
Supports both Excel and CSV files.

Usage:
    python script1.py input1.xlsx input2.csv output.xlsx --keys col1,col2
    python script1.py input1.csv input2.xlsx output.csv --keys id
"""

import pandas as pd
import argparse
import sys
from pathlib import Path


def read_file(filepath):
    """Read Excel or CSV file and return DataFrame"""
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # Determine file type by extension
    extension = filepath.suffix.lower()

    if extension in [".xlsx", ".xls"]:
        return pd.read_excel(filepath)
    elif extension == ".csv":
        return pd.read_csv(filepath)
    else:
        raise ValueError(
            f"Unsupported file format: {extension}. Only .xlsx, .xls, and .csv are supported."
        )


def write_file(df, filepath):
    """Write DataFrame to Excel or CSV file"""
    filepath = Path(filepath)
    extension = filepath.suffix.lower()

    # Create output directory if it doesn't exist
    filepath.parent.mkdir(parents=True, exist_ok=True)

    if extension in [".xlsx", ".xls"]:
        df.to_excel(filepath, index=False)
    elif extension == ".csv":
        df.to_csv(filepath, index=False)
    else:
        raise ValueError(
            f"Unsupported output format: {extension}. Only .xlsx, .xls, and .csv are supported."
        )


def validate_columns(df, columns, filename):
    """Validate that specified columns exist in the DataFrame"""
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        print(f"Error: Columns {missing_cols} not found in {filename}")
        print(f"Available columns in {filename}: {list(df.columns)}")
        sys.exit(1)


def match_rows(df1, df2, key_columns):
    """
    Keep only rows from df1 that have matching key values in df2

    Args:
        df1: Reference DataFrame (rows will be removed from this)
        df2: Comparison DataFrame (used to find matches)
        key_columns: List of column names to use for matching

    Returns:
        DataFrame with only matching rows from df1
    """
    # Validate that key columns exist in both DataFrames
    validate_columns(df1, key_columns, "input file 1")
    validate_columns(df2, key_columns, "input file 2")

    # Create a set of tuples from df2 key columns for efficient lookup
    df2_keys = set()
    for _, row in df2.iterrows():
        key_tuple = tuple(row[col] for col in key_columns)
        df2_keys.add(key_tuple)

    # Filter df1 to keep only rows where key combination exists in df2
    def row_matches(row):
        key_tuple = tuple(row[col] for col in key_columns)
        return key_tuple in df2_keys

    matched_df = df1[df1.apply(row_matches, axis=1)].copy()

    return matched_df


def main():
    parser = argparse.ArgumentParser(
        description="Remove rows from first document that don't match in second document",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python script1.py data1.xlsx data2.csv output.xlsx --keys id
    python script1.py file1.csv file2.csv result.csv --keys name,email
    python script1.py input1.xlsx input2.xlsx output.csv --keys user_id,date
        """,
    )

    parser.add_argument(
        "input1", help="First input file (Excel or CSV) - reference document"
    )
    parser.add_argument(
        "input2", help="Second input file (Excel or CSV) - comparison document"
    )
    parser.add_argument("output", help="Output file (Excel or CSV)")
    parser.add_argument(
        "--keys",
        "-k",
        help="Comma-separated list of column names to use as primary keys for matching",
        required=True,
    )

    args = parser.parse_args()

    try:
        # Parse key columns
        key_columns = [col.strip() for col in args.keys.split(",")]

        print(f"Reading input files...")
        print(f"  Input 1 (reference): {args.input1}")
        print(f"  Input 2 (comparison): {args.input2}")
        print(f"  Primary key columns: {key_columns}")

        # Read input files
        df1 = read_file(args.input1)
        df2 = read_file(args.input2)

        print(f"\nInput file statistics:")
        print(f"  {args.input1}: {len(df1)} rows, {len(df1.columns)} columns")
        print(f"  {args.input2}: {len(df2)} rows, {len(df2.columns)} columns")

        # Match rows
        print(f"\nMatching rows based on primary keys...")
        matched_df = match_rows(df1, df2, key_columns)

        # Write output
        write_file(matched_df, args.output)

        print(f"\nResults:")
        print(f"  Original rows in {args.input1}: {len(df1)}")
        print(f"  Matching rows found: {len(matched_df)}")
        print(f"  Rows removed: {len(df1) - len(matched_df)}")
        print(f"  Output saved to: {args.output}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
