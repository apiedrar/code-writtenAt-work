#!/usr/bin/env python3
"""
Excel Row Sorter - Sort rows in Excel 2 to match the order in Excel 1 using primary key column.

Usage:
    python sort_excel_rows.py <excel1_path> <excel2_path> <output_path> [--key-col KEY_COL]

Arguments:
    excel1_path: Path to Excel 1 (reference order)
    excel2_path: Path to Excel 2 (to be sorted)
    output_path: Path for output CSV file
    --key-col: Primary key column name or index (default: first column)

Examples:
    python sort_excel_rows.py data1.xlsx data2.xlsx sorted_output.csv
    python sort_excel_rows.py data1.xlsx data2.xlsx output.csv --key-col "ID"
    python sort_excel_rows.py data1.xlsx data2.xlsx output.csv --key-col 0
"""

import argparse
import pandas as pd
import sys
import os
from pathlib import Path

def load_excel_file(file_path, preserve_format=False):
    """Load Excel file and return DataFrame with format preservation."""
    try:
        # Enhanced loading with format preservation options
        if file_path.suffix.lower() in ['.xlsx', '.xlsm']:
            if preserve_format:
                # More aggressive type preservation
                df = pd.read_excel(
                    file_path, 
                    engine='openpyxl',
                    dtype=str,  # Load everything as string first
                    keep_default_na=False,  # Don't convert blanks to NaN
                    na_values=['']  # Only empty strings are NaN
                )
                return convert_types_intelligently(df)
            else:
                return pd.read_excel(file_path, engine='openpyxl')
        elif file_path.suffix.lower() == '.xls':
            if preserve_format:
                df = pd.read_excel(
                    file_path, 
                    engine='xlrd',
                    dtype=str,
                    keep_default_na=False,
                    na_values=['']
                )
                return convert_types_intelligently(df)
            else:
                return pd.read_excel(file_path, engine='xlrd')
        elif file_path.suffix.lower() == '.csv':
            if preserve_format:
                df = pd.read_csv(
                    file_path,
                    dtype=str,
                    keep_default_na=False,
                    na_values=['']
                )
                return convert_types_intelligently(df)
            else:
                return pd.read_csv(file_path)
        else:
            # Default fallback
            if preserve_format:
                df = pd.read_excel(
                    file_path,
                    dtype=str,
                    keep_default_na=False,
                    na_values=['']
                )
                return convert_types_intelligently(df)
            else:
                return pd.read_excel(file_path)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        sys.exit(1)

def convert_types_intelligently(df):
    """Convert string DataFrame to appropriate types while preserving original formatting."""
    print("Auto-detecting and preserving data types...")
    
    for col in df.columns:
        series = df[col].copy()
        
        # Skip completely empty columns
        if series.isna().all() or (series == '').all():
            continue
            
        # Get non-empty values for analysis
        non_empty = series[series != ''].dropna()
        if len(non_empty) == 0:
            continue
            
        # Check if column contains dates
        if is_date_column(non_empty):
            df[col] = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
            print(f"  Column '{col}': Detected as datetime")
            continue
            
        # Check if column is purely numeric (but preserve leading zeros)
        if is_numeric_column(non_empty):
            # Check for leading zeros first
            has_leading_zeros = any(val.startswith('0') and len(val) > 1 and val.isdigit() 
                                  for val in non_empty if isinstance(val, str))
            
            if has_leading_zeros:
                print(f"  Column '{col}': Keeping as text (has leading zeros)")
                continue  # Keep as string to preserve leading zeros
            
            # Try to convert to numeric
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Check if conversion was mostly successful (allow some NaN)
            success_rate = (numeric_series.notna().sum() / len(non_empty))
            if success_rate > 0.8:  # 80% success rate threshold
                # Determine if integer or float
                if numeric_series.dropna().apply(lambda x: x == int(x) if pd.notna(x) else True).all():
                    df[col] = numeric_series.astype('Int64')  # Nullable integer
                    print(f"  Column '{col}': Converted to integer")
                else:
                    df[col] = numeric_series.astype('float64')
                    print(f"  Column '{col}': Converted to float")
                continue
        
        # Check if column is boolean-like
        if is_boolean_column(non_empty):
            bool_mapping = {
                'true': True, 'false': False, 'yes': True, 'no': False,
                '1': True, '0': False, 'y': True, 'n': False,
                'TRUE': True, 'FALSE': False, 'YES': True, 'NO': False,
                'Y': True, 'N': False
            }
            df[col] = series.map(bool_mapping).fillna(series)
            print(f"  Column '{col}': Converted to boolean")
            continue
            
        # Keep as string (preserves original formatting)
        print(f"  Column '{col}': Keeping as text")
    
    return df

def is_date_column(series):
    """Check if a series contains date-like values."""
    if len(series) == 0:
        return False
        
    # Sample up to 100 values for performance
    sample = series.head(100) if len(series) > 100 else series
    
    date_count = 0
    for val in sample:
        if pd.isna(val) or val == '':
            continue
        try:
            pd.to_datetime(val, infer_datetime_format=True)
            date_count += 1
        except:
            pass
    
    # If more than 70% look like dates, treat as date column
    return (date_count / len(sample.dropna())) > 0.7

def is_numeric_column(series):
    """Check if a series contains numeric values."""
    if len(series) == 0:
        return False
        
    # Sample for performance
    sample = series.head(100) if len(series) > 100 else series
    
    numeric_count = 0
    for val in sample:
        if pd.isna(val) or val == '':
            continue
        try:
            float(val)
            numeric_count += 1
        except:
            pass
    
    # If more than 80% are numeric, treat as numeric
    return (numeric_count / len(sample.dropna())) > 0.8

def is_boolean_column(series):
    """Check if a series contains boolean-like values."""
    if len(series) == 0:
        return False
        
    bool_values = {
        'true', 'false', 'yes', 'no', '1', '0', 'y', 'n',
        'TRUE', 'FALSE', 'YES', 'NO', 'Y', 'N'
    }
    
    # Sample for performance
    sample = series.head(100) if len(series) > 100 else series
    non_empty = sample.dropna()
    
    if len(non_empty) == 0:
        return False
        
    bool_count = sum(1 for val in non_empty if str(val).strip() in bool_values)
    return (bool_count / len(non_empty)) > 0.8

def get_key_column(df, key_col_arg):
    """Determine the primary key column."""
    if key_col_arg is None:
        # Default to first column
        return df.columns[0]
    
    # Check if it's an integer (column index)
    try:
        col_index = int(key_col_arg)
        if 0 <= col_index < len(df.columns):
            return df.columns[col_index]
        else:
            print(f"Error: Column index {col_index} is out of range (0-{len(df.columns)-1})")
            sys.exit(1)
    except ValueError:
        # It's a column name
        if key_col_arg in df.columns:
            return key_col_arg
        else:
            print(f"Error: Column '{key_col_arg}' not found in dataframe")
            print(f"Available columns: {list(df.columns)}")
            sys.exit(1)

def sort_excel_rows(excel1_path, excel2_path, output_path, key_col=None):
    """Sort Excel 2 rows to match Excel 1 order using primary key column."""
    
    # Convert paths to Path objects
    excel1_path = Path(excel1_path)
    excel2_path = Path(excel2_path)
    output_path = Path(output_path)
    
    # Check if input files exist
    if not excel1_path.exists():
        print(f"Error: File {excel1_path} does not exist")
        sys.exit(1)
    
    if not excel2_path.exists():
        print(f"Error: File {excel2_path} does not exist")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading {excel1_path}...")
    df1 = load_excel_file(excel1_path)
    
    print(f"Loading {excel2_path}...")
    df2 = load_excel_file(excel2_path)
    
    print(f"Excel 1 shape: {df1.shape}")
    print(f"Excel 2 shape: {df2.shape}")
    
    # Determine primary key columns
    key_col1 = get_key_column(df1, key_col)
    key_col2 = get_key_column(df2, key_col)
    
    print(f"Using primary key column: '{key_col1}' in Excel 1, '{key_col2}' in Excel 2")
    
    # Check for duplicates in key columns
    if df1[key_col1].duplicated().any():
        duplicates = df1[df1[key_col1].duplicated()][key_col1].tolist()
        print(f"Warning: Duplicate keys found in Excel 1: {duplicates[:5]}{'...' if len(duplicates) > 5 else ''}")
    
    if df2[key_col2].duplicated().any():
        duplicates = df2[df2[key_col2].duplicated()][key_col2].tolist()
        print(f"Warning: Duplicate keys found in Excel 2: {duplicates[:5]}{'...' if len(duplicates) > 5 else ''}")
    
    # Create the order mapping from Excel 1
    print("Creating sort order mapping...")
    order_mapping = {key: idx for idx, key in enumerate(df1[key_col1])}
    
    # Add order column to Excel 2
    df2['_sort_order'] = df2[key_col2].map(order_mapping)
    
    # Check for missing keys
    missing_keys = df2[df2['_sort_order'].isna()]
    if not missing_keys.empty:
        print(f"Warning: {len(missing_keys)} keys from Excel 2 not found in Excel 1")
        print(f"These rows will be placed at the end: {missing_keys[key_col2].tolist()[:5]}{'...' if len(missing_keys) > 5 else ''}")
        # Assign high sort values to missing keys
        df2['_sort_order'] = df2['_sort_order'].fillna(len(df1) + df2.index)
    
    # Sort Excel 2 by the order from Excel 1
    print("Sorting rows...")
    df2_sorted = df2.sort_values('_sort_order').drop('_sort_order', axis=1)
    
    # Reset index
    df2_sorted = df2_sorted.reset_index(drop=True)
    
    # Save to CSV with non-formatted headers
    print(f"Saving sorted data to {output_path}...")
    df2_sorted.to_csv(output_path, index=False, header=True)
    
    print(f"✅ Successfully sorted and saved {len(df2_sorted)} rows to {output_path}")
    print(f"Output shape: {df2_sorted.shape}")

def main():
    parser = argparse.ArgumentParser(
        description="Sort rows in Excel 2 to match the order in Excel 1 using primary key column",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sort_excel_rows.py data1.xlsx data2.xlsx sorted_output.csv
  python sort_excel_rows.py data1.xlsx data2.xlsx output.csv --key-col "ID"
  python sort_excel_rows.py data1.xlsx data2.xlsx output.csv --key-col 0
        """
    )
    
    parser.add_argument('excel1_path', help='Path to Excel 1 (reference order)')
    parser.add_argument('excel2_path', help='Path to Excel 2 (to be sorted)')
    parser.add_argument('output_path', help='Path for output CSV file')
    parser.add_argument('--key-col', help='Primary key column name or index (default: first column)')
    
    args = parser.parse_args()
    
    try:
        sort_excel_rows(args.excel1_path, args.excel2_path, args.output_path, args.key_col)
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()