#!/usr/bin/env python3
"""
Large Data File Processor
A flexible script for processing large CSV and Excel files with filtering capabilities.
"""

import argparse
import pandas as pd
import os
import re
import sys
from pathlib import Path
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self):
        self.supported_formats = [".csv", ".xlsx", ".xls"]

    def read_file(self, file_path, **kwargs):
        """Read CSV or Excel file efficiently"""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_ext = file_path.suffix.lower()

        if file_ext == ".csv":
            # For large CSV files, read in chunks if needed
            try:
                return pd.read_csv(file_path, **kwargs)
            except Exception as e:
                logger.error(f"Error reading CSV file: {e}")
                raise

        elif file_ext in [".xlsx", ".xls"]:
            try:
                return pd.read_excel(file_path, **kwargs)
            except Exception as e:
                logger.error(f"Error reading Excel file: {e}")
                raise
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

    def parse_filters(self, filter_string):
        """Parse filter string into column-condition pairs"""
        if not filter_string:
            return []

        filters = []
        try:
            # Expected format: "column1:exact:value1,column2:contains:value2,column3:regex:pattern"
            filter_parts = filter_string.split(",")

            for part in filter_parts:
                if ":" not in part:
                    continue

                components = part.split(":")
                if len(components) < 3:
                    logger.warning(f"Invalid filter format: {part}")
                    continue

                column = components[0].strip()
                condition = components[1].strip().lower()
                value = ":".join(
                    components[2:]
                ).strip()  # Join back in case value contains ':'

                filters.append(
                    {"column": column, "condition": condition, "value": value}
                )

        except Exception as e:
            logger.error(f"Error parsing filters: {e}")
            raise ValueError(f"Invalid filter format: {filter_string}")

        return filters

    def apply_filter(self, df, filter_config):
        """Apply a single filter to the dataframe"""
        column = filter_config["column"]
        condition = filter_config["condition"]
        value = filter_config["value"]

        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in data")
            return df

        try:
            if condition == "exact":
                mask = df[column].astype(str) == value
            elif condition == "contains":
                mask = df[column].astype(str).str.contains(value, case=False, na=False)
            elif condition == "regex":
                mask = (
                    df[column]
                    .astype(str)
                    .str.contains(value, regex=True, case=False, na=False)
                )
            elif condition == "starts_with":
                mask = df[column].astype(str).str.startswith(value, na=False)
            elif condition == "ends_with":
                mask = df[column].astype(str).str.endswith(value, na=False)
            elif condition == "not_exact":
                mask = df[column].astype(str) != value
            elif condition == "not_contains":
                mask = ~df[column].astype(str).str.contains(value, case=False, na=False)
            else:
                logger.warning(f"Unknown condition: {condition}")
                return df

            return df[mask]

        except Exception as e:
            logger.error(f"Error applying filter {filter_config}: {e}")
            return df

    def apply_filters(self, df, filters):
        """Apply all filters to the dataframe"""
        if not filters:
            return df

        filtered_df = df.copy()

        for filter_config in filters:
            logger.info(f"Applying filter: {filter_config}")
            filtered_df = self.apply_filter(filtered_df, filter_config)
            logger.info(f"Rows after filter: {len(filtered_df)}")

        return filtered_df

    def perform_action(self, df, action, output_path=None):
        """Perform the specified action on the dataframe"""
        results = {}

        if action == "count":
            results["total_rows"] = len(df)
            results["total_columns"] = len(df.columns)
            results["column_names"] = list(df.columns)

        elif action == "summary":
            results["total_rows"] = len(df)
            results["total_columns"] = len(df.columns)
            results["column_info"] = {}

            for col in df.columns:
                results["column_info"][col] = {
                    "dtype": str(df[col].dtype),
                    "non_null_count": df[col].notna().sum(),
                    "null_count": df[col].isna().sum(),
                    "unique_count": df[col].nunique(),
                }

        elif action == "export":
            if output_path:
                output_path = Path(output_path)
                if output_path.suffix.lower() == ".csv":
                    df.to_csv(output_path, index=False)
                else:
                    df.to_excel(output_path, index=False)
                results["exported_file"] = str(output_path)
                results["exported_rows"] = len(df)
            else:
                logger.error("Output path required for export action")

        return results

    def process_file(self, input_path, filters=None, action="count", output_path=None):
        """Main processing function"""
        logger.info(f"Processing file: {input_path}")

        # Read the file
        df = self.read_file(input_path)
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")

        # Apply filters
        if filters:
            df = self.apply_filters(df, filters)
            logger.info(f"After filtering: {len(df)} rows")

        # Perform action
        results = self.perform_action(df, action, output_path)

        return results


def main():
    parser = argparse.ArgumentParser(
        description="Process large CSV and Excel files with filtering capabilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Count all rows
  python script.py -i data.csv -a count
  
  # Filter and count
  python script.py -i data.csv -f "status:exact:active,name:contains:john" -a count
  
  # Use regex filter
  python script.py -i data.xlsx -f "email:regex:.*@gmail\.com" -a count
  
  # Export filtered data
  python script.py -i data.csv -f "age:exact:25" -a export -o filtered_data.csv
  
Filter conditions:
  - exact: Exact match
  - contains: Text contains value (case-insensitive)
  - regex: Regular expression match
  - starts_with: Text starts with value
  - ends_with: Text ends with value
  - not_exact: Does not match exactly
  - not_contains: Does not contain value
        """,
    )

    parser.add_argument(
        "-i", "--input", required=True, help="Input file path (CSV or Excel)"
    )

    parser.add_argument("-o", "--output", help="Output file path (for export action)")

    parser.add_argument(
        "-f",
        "--filters",
        help="Filters in format: column1:condition1:value1,column2:condition2:value2",
    )

    parser.add_argument(
        "-a",
        "--action",
        default="count",
        choices=["count", "summary", "export"],
        help="Action to perform (default: count)",
    )

    parser.add_argument(
        "--sheet", help="Sheet name for Excel files (default: first sheet)"
    )

    parser.add_argument(
        "--encoding", help="File encoding for CSV files (default: utf-8)"
    )

    parser.add_argument("--sep", help="Separator for CSV files (default: comma)")

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        processor = DataProcessor()

        # Parse filters
        filters = processor.parse_filters(args.filters) if args.filters else None

        # Prepare read kwargs
        read_kwargs = {}
        if args.sheet:
            read_kwargs["sheet_name"] = args.sheet
        if args.encoding:
            read_kwargs["encoding"] = args.encoding
        if args.sep:
            read_kwargs["sep"] = args.sep

        # Process file
        results = processor.process_file(
            input_path=args.input,
            filters=filters,
            action=args.action,
            output_path=args.output,
        )

        # Display results
        print("\n" + "=" * 50)
        print("PROCESSING RESULTS")
        print("=" * 50)

        for key, value in results.items():
            if isinstance(value, dict):
                print(f"{key.upper()}:")
                for subkey, subvalue in value.items():
                    print(f"  {subkey}: {subvalue}")
            elif isinstance(value, list):
                print(f"{key.upper()}: {', '.join(map(str, value))}")
            else:
                print(f"{key.upper()}: {value}")

        print("=" * 50)

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
