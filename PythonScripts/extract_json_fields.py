#!/usr/bin/env python3
"""
Extract fields from JSON strings in Excel documents.

Usage:
    python extract_json_fields.py input.xlsx output.xlsx field1 [field2 ...]

Example:
    python extract_json_fields.py data.xlsx output.xlsx "_advanced_info.claropagos.comercio_uuid" "timestamp"
"""

import sys
import json
import pandas as pd
from pathlib import Path


def get_nested_field(data, field_path):
    """
    Extract a nested field from a dictionary using dot notation.

    Args:
        data: Dictionary to extract from
        field_path: Dot-separated path to field (e.g., "a.b.c")

    Returns:
        The value at the specified path, or None if not found
    """
    keys = field_path.split(".")
    value = data

    try:
        for key in keys:
            value = value[key]
        return value
    except (KeyError, TypeError, IndexError):
        return None


def extract_json_fields(input_file, output_file, fields):
    """
    Extract specified fields from JSON strings in Excel file.

    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file
        fields: List of field paths to extract
    """
    # Read the Excel file
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)

    if df.empty:
        print("Error: Input file is empty")
        sys.exit(1)

    # Assume JSON data is in the first column
    json_column = df.columns[0]
    print(f"Processing column: {json_column}")
    print(f"Total rows: {len(df)}")

    # Extract each requested field
    for field in fields:
        print(f"Extracting field: {field}")

        def extract_field(json_string):
            try:
                data = json.loads(json_string)
                return get_nested_field(data, field)
            except (json.JSONDecodeError, TypeError) as e:
                return f"Error: {str(e)}"

        df[field] = df[json_column].apply(extract_field)

    # Save to output file
    print(f"Saving to {output_file}...")
    df.to_excel(output_file, index=False)
    print(f"Done! Extracted {len(fields)} field(s) from {len(df)} row(s)")


def main():
    if len(sys.argv) < 4:
        print(
            "Usage: python extract_json_fields.py <input_file> <output_file> <field1> [field2 ...]"
        )
        print("\nExample:")
        print(
            '  python extract_json_fields.py data.xlsx output.xlsx "_advanced_info.claropagos.comercio_uuid"'
        )
        print("\nField format:")
        print("  Use dot notation for nested fields: 'parent.child.grandchild'")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    fields = sys.argv[3:]

    # Validate input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found")
        sys.exit(1)

    # Extract fields
    try:
        extract_json_fields(input_file, output_file, fields)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
