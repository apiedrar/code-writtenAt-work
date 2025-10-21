#!/usr/bin/env python3
"""
Script to match rows using hierarchical key matching:
1. Match on primary key
2. If primary key has duplicates, add phone number to matching
3. If still duplicated, add date field to matching

Usage:
    python hierarchical_match.py input1.xlsx input2.csv output.xlsx --primary id --phone phone_number --date timestamp
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta


def read_file(filepath):
    """Read Excel or CSV file and return DataFrame"""
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

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
    missing_cols = [col for col in columns if col and col not in df.columns]
    if missing_cols:
        print(f"Error: Columns {missing_cols} not found in {filename}")
        print(f"Available columns in {filename}: {list(df.columns)}")
        sys.exit(1)


def normalize_date(date_val):
    """Normalize date value to datetime object, handling various input types"""
    if pd.isna(date_val):
        return None

    try:
        # Try to parse as datetime
        if isinstance(date_val, datetime):
            return date_val
        elif isinstance(date_val, pd.Timestamp):
            return date_val.to_pydatetime()
        else:
            # Try parsing string
            return pd.to_datetime(str(date_val))
    except:
        return None


def dates_within_range(date1, date2, max_seconds=3):
    """Check if two dates are within max_seconds of each other"""
    if date1 is None or date2 is None:
        return False

    try:
        diff = abs((date1 - date2).total_seconds())
        return diff <= max_seconds
    except:
        return False


def hierarchical_match(
    df1, df2, primary_key, phone_col=None, date_col=None, date_tolerance=3, debug=False
):
    """
    Match rows using hierarchical key strategy:
    - Always use primary key
    - Add phone number if provided and primary key has duplicates
    - Add date field if provided and primary+phone still has duplicates (within tolerance)

    Args:
        df1: Reference DataFrame (rows will be filtered)
        df2: Comparison DataFrame (used to find matches)
        primary_key: Primary key column name
        phone_col: Phone number column name (optional)
        date_col: Date column name (optional)
        date_tolerance: Maximum seconds difference for date matching (default: 3)
        debug: Enable debug output (default: False)

    Returns:
        Tuple of (matched_df, unmatched_df, debug_info)
    """
    # Validate columns
    cols_to_validate = [primary_key]
    if phone_col:
        cols_to_validate.append(phone_col)
    if date_col:
        cols_to_validate.append(date_col)

    validate_columns(df1, cols_to_validate, "input file 1")
    validate_columns(df2, cols_to_validate, "input file 2")

    print(f"\nAnalyzing data structure...")
    print(f"  Matching strategy: primary key", end="")
    if phone_col:
        print(f" + phone", end="")
    if date_col:
        print(f" + date (±{date_tolerance}s)", end="")
    print()

    # Find duplicates in df1 based on primary key
    df1_primary_counts = df1[primary_key].value_counts()
    df1_duplicated_primary = set(df1_primary_counts[df1_primary_counts > 1].index)

    # Find duplicates in df2 based on primary key
    df2_primary_counts = df2[primary_key].value_counts()
    df2_duplicated_primary = set(df2_primary_counts[df2_primary_counts > 1].index)

    print(f"  Primary keys with duplicates in file 1: {len(df1_duplicated_primary)}")
    print(f"  Primary keys with duplicates in file 2: {len(df2_duplicated_primary)}")

    # All primary keys that have duplicates in either file
    duplicated_primary = df1_duplicated_primary | df2_duplicated_primary

    # Build lookup structure from df2
    df2_lookup = defaultdict(dict)

    for _, row in df2.iterrows():
        pk = row[primary_key]

        if pk in duplicated_primary and (phone_col or date_col):
            # Use hierarchical keys based on what's provided
            if not phone_col:
                # Only primary + date
                date = normalize_date(row[date_col]) if date_col else None
                if pk not in df2_lookup:
                    df2_lookup[pk] = []
                if date is not None:
                    df2_lookup[pk].append(date)
            else:
                # Primary + phone (+ date if provided)
                phone = row[phone_col]

                if pk not in df2_lookup:
                    df2_lookup[pk] = {}
                if phone not in df2_lookup[pk]:
                    if date_col:
                        df2_lookup[pk][phone] = []
                    else:
                        df2_lookup[pk][phone] = True

                if date_col:
                    date = normalize_date(row[date_col])
                    if date is not None:
                        df2_lookup[pk][phone].append(date)
        else:
            # Simple primary key only
            df2_lookup[pk] = True

    # Match rows from df1
    matched_indices = []
    unmatched_indices = []
    match_levels = {
        "primary_only": 0,
        "primary_phone": 0,
        "primary_phone_date": 0,
        "primary_date": 0,
    }
    debug_info = {"unmatched_reasons": [], "duplicate_examples": []}

    for idx, row in df1.iterrows():
        pk = row[primary_key]
        matched = False
        reason = None

        # Check if primary key exists in df2
        if pk not in df2_lookup:
            reason = f"Primary key '{pk}' not found in file 2"
            unmatched_indices.append(idx)
            if debug:
                debug_info["unmatched_reasons"].append(
                    {"row_index": idx, "primary_key": pk, "reason": reason}
                )
            continue

        if pk in duplicated_primary and (phone_col or date_col):

            if not phone_col and date_col:
                # Only primary + date matching
                date1 = normalize_date(row[date_col])
                if date1 is not None and isinstance(df2_lookup[pk], list):
                    for date2 in df2_lookup[pk]:
                        if dates_within_range(date1, date2, date_tolerance):
                            matched = True
                            match_levels["primary_date"] += 1
                            break
                    if not matched:
                        reason = f"Primary key '{pk}' is duplicate, date doesn't match within ±{date_tolerance}s"
                else:
                    reason = f"Primary key '{pk}' is duplicate, invalid date in file 1"

            elif phone_col and not date_col:
                # Primary + phone matching
                phone = row[phone_col]
                if phone in df2_lookup[pk]:
                    matched = True
                    match_levels["primary_phone"] += 1
                else:
                    reason = f"Primary key '{pk}' is duplicate, phone '{phone}' not found in file 2 for this primary key"

            else:
                # Primary + phone + date matching
                phone = row[phone_col]
                date1 = normalize_date(row[date_col])

                if phone not in df2_lookup[pk]:
                    reason = f"Primary key '{pk}' is duplicate, phone '{phone}' not found in file 2"
                elif date1 is not None and isinstance(df2_lookup[pk][phone], list):
                    for date2 in df2_lookup[pk][phone]:
                        if dates_within_range(date1, date2, date_tolerance):
                            matched = True
                            match_levels["primary_phone_date"] += 1
                            break
                    if not matched:
                        reason = f"Primary key '{pk}' + phone '{phone}' found, but date doesn't match within ±{date_tolerance}s"
                elif not isinstance(df2_lookup[pk][phone], list):
                    # Date column provided but this entry has no valid dates
                    matched = True
                    match_levels["primary_phone"] += 1
                else:
                    reason = f"Primary key '{pk}' + phone '{phone}' found, but invalid date in file 1"

            if matched:
                matched_indices.append(idx)
            else:
                unmatched_indices.append(idx)
                if debug and reason:
                    debug_info["unmatched_reasons"].append(
                        {
                            "row_index": idx,
                            "primary_key": pk,
                            "phone": row[phone_col] if phone_col else None,
                            "date": str(row[date_col]) if date_col else None,
                            "reason": reason,
                        }
                    )
        else:
            # Simple primary key match
            matched = True
            matched_indices.append(idx)
            match_levels["primary_only"] += 1

    matched_df = df1.loc[matched_indices].copy()
    unmatched_df = df1.loc[unmatched_indices].copy()

    # Print matching statistics
    print(f"\nMatching statistics:")
    print(f"  Matched by primary key only: {match_levels['primary_only']}")
    if phone_col and not date_col:
        print(f"  Matched by primary + phone: {match_levels['primary_phone']}")
    if date_col and not phone_col:
        print(
            f"  Matched by primary + date (±{date_tolerance}s): {match_levels['primary_date']}"
        )
    if phone_col and date_col:
        print(f"  Matched by primary + phone: {match_levels['primary_phone']}")
        print(
            f"  Matched by primary + phone + date (±{date_tolerance}s): {match_levels['primary_phone_date']}"
        )
    print(f"  Total matched: {len(matched_df)}")
    print(f"  Total unmatched: {len(unmatched_df)}")

    if debug and len(debug_info["unmatched_reasons"]) > 0:
        print(f"\n  Showing first 10 unmatched rows:")
        for info in debug_info["unmatched_reasons"][:10]:
            print(f"    Row {info['row_index']}: {info['reason']}")
        if len(debug_info["unmatched_reasons"]) > 10:
            print(
                f"    ... and {len(debug_info['unmatched_reasons']) - 10} more unmatched rows"
            )

    return matched_df, unmatched_df, debug_info


def main():
    parser = argparse.ArgumentParser(
        description="Match rows using hierarchical key strategy (primary key, phone, date)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python hierarchical_match.py data1.xlsx data2.csv output.xlsx --primary id
    python hierarchical_match.py file1.csv file2.csv result.csv --primary name --phone mobile
    python hierarchical_match.py input1.xlsx input2.xlsx output.csv --primary user_id --phone phone --date created_at
    python hierarchical_match.py input1.xlsx input2.xlsx output.csv --primary user_id --phone phone --date created_at --tolerance 5
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
        "--primary",
        "-p",
        help="Primary key column name",
        required=True,
    )
    parser.add_argument(
        "--phone",
        "-ph",
        help="Phone number column name (optional - used for duplicate primary keys)",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--date",
        "-d",
        help="Date column name (optional - format: YYYY-MM-DD HH:MM:SS)",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--tolerance",
        "-t",
        help="Maximum seconds difference for date matching (default: 3, only used if --date is provided)",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode to show detailed matching information and export unmatched rows",
    )

    args = parser.parse_args()

    try:
        print(f"Reading input files...")
        print(f"  Input 1 (reference): {args.input1}")
        print(f"  Input 2 (comparison): {args.input2}")
        print(f"  Primary key: {args.primary}")
        if args.phone:
            print(f"  Phone column: {args.phone}")
        if args.date:
            print(f"  Date column: {args.date}")
            print(f"  Date tolerance: ±{args.tolerance} seconds")

        # Read input files
        df1 = read_file(args.input1)
        df2 = read_file(args.input2)

        print(f"\nInput file statistics:")
        print(f"  {args.input1}: {len(df1)} rows, {len(df1.columns)} columns")
        print(f"  {args.input2}: {len(df2)} rows, {len(df2.columns)} columns")

        # Perform hierarchical matching
        matched_df, unmatched_df, debug_info = hierarchical_match(
            df1, df2, args.primary, args.phone, args.date, args.tolerance, args.debug
        )

        # Write output
        write_file(matched_df, args.output)

        print(f"\nResults:")
        print(f"  Original rows in {args.input1}: {len(df1)}")
        print(f"  Matching rows found: {len(matched_df)}")
        print(f"  Rows removed: {len(df1) - len(matched_df)}")
        print(f"  Output saved to: {args.output}")

        # Save unmatched rows if debug mode is enabled
        if args.debug and len(unmatched_df) > 0:
            output_path = Path(args.output)
            unmatched_path = (
                output_path.parent / f"{output_path.stem}_UNMATCHED{output_path.suffix}"
            )
            write_file(unmatched_df, unmatched_path)
            print(f"\n  Debug: Unmatched rows saved to: {unmatched_path}")

            # Also save detailed debug info
            debug_path = output_path.parent / f"{output_path.stem}_DEBUG.txt"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(f"Debug Information for {args.output}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Total unmatched rows: {len(unmatched_df)}\n\n")
                f.write("Unmatched Row Details:\n")
                f.write("-" * 80 + "\n")
                for info in debug_info["unmatched_reasons"]:
                    f.write(f"\nRow Index: {info['row_index']}\n")
                    f.write(f"Primary Key: {info['primary_key']}\n")
                    if "phone" in info and info["phone"]:
                        f.write(f"Phone: {info['phone']}\n")
                    if "date" in info and info["date"]:
                        f.write(f"Date: {info['date']}\n")
                    f.write(f"Reason: {info['reason']}\n")
            print(f"  Debug: Detailed report saved to: {debug_path}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
