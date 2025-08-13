import pandas as pd
import json
import argparse
import sys
import os
from pathlib import Path

def extract_json_data(df):
    """
    Extract data from JSON in the response_body column and return processed DataFrame
    """
    extracted_data = []
    
    for index, row in df.iterrows():
        try:
            json_data = json.loads(row['response_body'])
            
            # Create a dictionary with extracted data
            extracted = {
                'reference_code': json_data.get('clientReferenceInformation', {}).get('code'),
                'status': json_data.get('status'),
                'risk_score': json_data.get('riskInformation', {}).get('score', {}).get('result'),
                'early_decision': json_data.get('riskInformation', {}).get('profile', {}).get('earlyDecision'),
                'rejection_reason': json_data.get('errorInformation', {}).get('reason'),
                'payment_scheme': json_data.get('paymentInformation', {}).get('scheme'),
                'payment_bin': json_data.get('paymentInformation', {}).get('bin'),
                'emailage_score': json_data.get('riskInformation', {}).get('providers', {}).get('emailage', {}).get('ea_score'),
                'elephant_decision': json_data.get('riskInformation', {}).get('providers', {}).get('elephant', {}).get('decision')
                # You can add more fields as needed
            }
            
            # Add original values to maintain context
            extracted.update(row.to_dict())
            extracted_data.append(extracted)
        except Exception as e:
            # If there's an error parsing JSON, keep original data
            print(f"Warning: Error parsing JSON in row {index}: {e}")
            extracted_data.append(row.to_dict())
    
    return pd.DataFrame(extracted_data)

def process_file(input_path, output_path):
    """
    Process the input file and save results to output path
    """
    input_file = Path(input_path)
    output_file = Path(output_path)
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file '{input_path}' does not exist.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Determine file type and load data
    file_extension = input_file.suffix.lower()
    
    try:
        if file_extension in ['.xlsx', '.xls']:
            # Load Excel file and check all sheets
            excel_file = pd.ExcelFile(input_path)
            sheet_names = excel_file.sheet_names
            
            print(f"Found {len(sheet_names)} sheet(s): {sheet_names}")
            
            # Process each sheet
            all_processed_data = {}
            
            for sheet_name in sheet_names:
                print(f"Processing sheet: {sheet_name}")
                df = pd.read_excel(input_path, sheet_name=sheet_name)
                
                # Check if response_body column exists
                if 'response_body' not in df.columns:
                    print(f"Warning: 'response_body' column not found in sheet '{sheet_name}'. Skipping JSON extraction.")
                    all_processed_data[sheet_name] = df
                else:
                    processed_df = extract_json_data(df)
                    all_processed_data[sheet_name] = processed_df
            
            # Save results
            output_extension = output_file.suffix.lower()
            
            if output_extension == '.csv':
                # If output is CSV and there are multiple sheets, combine them or save the first one
                if len(all_processed_data) == 1:
                    list(all_processed_data.values())[0].to_csv(output_path, index=False)
                    print(f"Results saved to: {output_path}")
                else:
                    # Save each sheet as a separate CSV file
                    for sheet_name, data in all_processed_data.items():
                        sheet_output_path = output_file.parent / f"{output_file.stem}_{sheet_name}{output_file.suffix}"
                        data.to_csv(sheet_output_path, index=False)
                        print(f"Sheet '{sheet_name}' saved to: {sheet_output_path}")
            else:
                # Save as Excel with multiple sheets
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for sheet_name, data in all_processed_data.items():
                        data.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Results saved to: {output_path}")
                
        elif file_extension == '.csv':
            # Load CSV file
            print("Processing CSV file")
            df = pd.read_csv(input_path)
            
            # Check if response_body column exists
            if 'response_body' not in df.columns:
                print("Warning: 'response_body' column not found. Skipping JSON extraction.")
                processed_df = df
            else:
                processed_df = extract_json_data(df)
            
            # Save results
            output_extension = output_file.suffix.lower()
            if output_extension == '.csv':
                processed_df.to_csv(output_path, index=False)
            else:
                processed_df.to_excel(output_path, index=False)
            
            print(f"Results saved to: {output_path}")
            
        else:
            print(f"Error: Unsupported file format '{file_extension}'. Please use .xlsx, .xls, or .csv files.")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Extract JSON data from response_body column in Excel/CSV files')
    parser.add_argument('input_path', help='Path to input Excel (.xlsx/.xls) or CSV file')
    parser.add_argument('output_path', help='Path to output file (Excel or CSV)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Input file: {args.input_path}")
        print(f"Output file: {args.output_path}")
    
    process_file(args.input_path, args.output_path)

if __name__ == "__main__":
    main()