import json
import os
import time
import pandas as pd
import csv
import sys
import logging as python_logging  # Renamed to avoid conflicts
from CyberSource import *
from pathlib import Path
from importlib.machinery import SourceFileLoader
import gc
import urllib3
import argparse
from datetime import datetime

# Configure connection pooling for urllib3 (used by requests/CyberSource)
urllib3.PoolManager(maxsize=100, block=True)

# Disable or configure logging to avoid errors
python_logging.getLogger('CyberSource').setLevel(python_logging.ERROR)
python_logging.getLogger('urllib3').setLevel(python_logging.ERROR)
python_logging.getLogger('requests').setLevel(python_logging.ERROR)

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.getcwd(), "Logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Try to create empty log files to avoid FileNotFoundError
for i in range(1, 11):  # Create logs from 1 to 10
    log_file = os.path.join(log_dir, f"cybs.log.{i}")
    if not os.path.exists(log_file):
        try:
            with open(log_file, 'w') as f:
                pass  # Create empty file
        except:
            pass  # Ignore errors when creating files

# Load CyberSource configuration
config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

# Global configuration for reuse
config_obj = configuration.Configuration()
client_config = config_obj.get_configuration()

def del_none(d):
    """Removes None values from a dictionary recursively"""
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d

def format_datetime(datetime_str):
    """
    Converts datetime string to ISO format with timezone offset
    Input: "2025-08-03 15:14:41" 
    Output: "2025-08-03T15:14:41-06:00"
    """
    try:
        # Parse the datetime string
        dt = datetime.strptime(str(datetime_str).strip(), "%Y-%m-%d %H:%M:%S")
        # Format to ISO with timezone (assuming Mexico timezone -06:00)
        formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S-06:00")
        return formatted_dt
    except Exception as e:
        print(f"Error formatting datetime '{datetime_str}': {e}")
        # Return current datetime as fallback
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S-06:00")

def determine_order_type(quantity):
    """
    Determines order type based on quantity
    quantity = 1: "single item"
    quantity > 1: "multiple items"
    """
    try:
        qty = int(float(str(quantity).strip())) if str(quantity).strip() else 1
        return "single item" if qty == 1 else "multiple items"
    except:
        return "single item"  # Default fallback

def process_transaction(row, api_instance):
    """Processes a transaction through CyberSource with time measurement"""
    try:
        # Convert values to string to ensure compatibility
        for key in row:
            if pd.isna(row[key]):
                row[key] = ""
            else:
                row[key] = str(row[key])
        
        clientReferenceInformation = Riskv1decisionsClientReferenceInformation(
            code=row.get('id', '')
        )

        paymentInformationCard = Riskv1decisionsPaymentInformationCard(
            bin=row.get('bin', ''),
        )
        paymentInformation = Riskv1decisionsPaymentInformation(
            card=paymentInformationCard.__dict__
        )

        orderInformationAmountDetails = Riskv1decisionsOrderInformationAmountDetails(
            currency=row.get('currency__id', 'MXN'),
            total_amount=row.get('local_currency_amt', '0')
        )
        
        orderInformationShipTo = Riskv1decisionsOrderInformationShipTo(
            address1=row.get('shipping_address', ''),
            administrative_area=row.get('shipping_state', ''),
            country=row.get('shipping_country', ''),
            locality=row.get('shipping_city', ''),
            phone_number=row.get('shipping_phone_number', ''),
            postal_code=row.get('shipping_zip_code', '')
        )
        
        orderInformationBillTo = Riskv1decisionsOrderInformationBillTo(
            address1=str(row.get('address_number', '')) + ' ' + str(row.get('address_street', '')),
            administrative_area=row.get('address_state', ''),
            country=row.get('address_country', ''),
            locality=row.get('address_city', ''),
            first_name=row.get('first_name', ''),
            last_name=row.get('last_name', ''),
            phone_number=row.get('phone_number', ''),
            email=row.get('email', ''),
            postal_code=row.get('address_zip_code', '')
        )

        # Modified lineItems to include unitPrice and use proper array format
        orderInformationLineItems = Riskv1addressverificationsOrderInformationLineItems(
            quantity=row.get('items_quantity', '1'),
            product_name=row.get('item_name', ''),  # Keep original value without conversion
            unit_price=row.get('local_currency_amt', '0')  # Add unit price
        )

        # Create merchantDefinedInformation array
        merchantDefinedInfo = []
        
        # Add dateTime field
        if row.get('date_time'):
            merchantDefinedInfo.append({
                "key": "1",
                "value": format_datetime(row['date_time'])
            })
        
        # Add orderType field based on quantity
        order_type = determine_order_type(row.get('items_quantity', '1'))
        merchantDefinedInfo.append({
            "key": "2", 
            "value": order_type
        })

        orderInformation = Riskv1decisionsOrderInformation(
            amount_details=orderInformationAmountDetails.__dict__,
            ship_to=orderInformationShipTo.__dict__,
            bill_to=orderInformationBillTo.__dict__,
            line_items=[orderInformationLineItems.__dict__],  # Make it an array
        )
        
        requestObj = CreateBundledDecisionManagerCaseRequest(
            client_reference_information=clientReferenceInformation.__dict__,
            payment_information=paymentInformation.__dict__,
            order_information=orderInformation.__dict__,
            merchant_defined_information=merchantDefinedInfo  # Add merchantDefinedInformation
        )
        
        requestObj = del_none(requestObj.__dict__)
        requestObj = json.dumps(requestObj)
            
        # Measure API call time
        try:
            start_time = time.time()
            return_data, status, body = api_instance.create_bundled_decision_manager_case(requestObj)
            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2)  # Convert to milliseconds and round
            return status, body, response_time
        except Exception as api_error:
            end_time = time.time()
            response_time = round((end_time - start_time) * 1000, 2) if 'start_time' in locals() else 0
            # Capture specific API errors
            print(f"Error in API call: {api_error}")
            return (api_error.status if hasattr(api_error, 'status') else 999), str(api_error), response_time
            
    except Exception as e:
        # Capture any other error in preparation
        import traceback
        print(f"Error preparing transaction: {e}")
        traceback.print_exc()
        return 999, str(e), 0

def process_in_batches(rows, output_csv, fieldnames, batch_size=50):
    """Processes records in batches for better resource management"""
    total_rows = len(rows)
    
    # Create a single API instance for reuse
    api_instance = DecisionManagerApi(client_config)
    
    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = rows[batch_start:batch_end]
        results = []
        
        # Process the batch
        for i, row in enumerate(batch):
            row_index = batch_start + i
            
            # Process the transaction
            status, body, response_time = process_transaction(row, api_instance)
            
            # Add response to record
            result = row.copy()  # Create a copy to not modify the original
            result['response_status'] = status
            result['response_body'] = body
            result['response_time_ms'] = response_time
            results.append(result)
            
            # Show progress with email and response time included
            email = row.get('email', 'N/A')
            quantity = row.get('items_quantity', '1')
            order_type = determine_order_type(quantity)
            print(f"Transaction {row_index+1}/{total_rows}: Status {status}, Email: {email}, Quantity: {quantity} ({order_type}), Time: {response_time}ms")
            
            # Small pause to not overload
            time.sleep(0.025)
        
        # Write entire batch together to CSV
        with open(output_csv, 'a', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(results)
        
        # Clean memory after each batch
        del results
        gc.collect()
        
        print(f"Processed batch {batch_start+1}-{batch_end} of {total_rows}")

def excel_to_csv_processor(input_excel, output_csv):
    """
    Processes an Excel file, extracts data, makes API calls and saves results to CSV
    This method avoids keeping Excel open during entire processing
    """
    try:
        # Convert Excel to list of dictionaries (single Excel operation)
        print(f"Reading Excel file: {input_excel}")
        df = pd.read_excel(input_excel)
        rows = df.to_dict('records')
        print(f"Excel file converted to {len(rows)} records")
        
        # Close any connection with Excel
        del df
        gc.collect()
        
        # Prepare output CSV file with new time column
        fieldnames = list(rows[0].keys()) + ['response_status', 'response_body', 'response_time_ms']
        
        # Create CSV file with headers
        with open(output_csv, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        
        # Process records in batches for better resource management
        process_in_batches(rows, output_csv, fieldnames, batch_size=50)
        
        print(f"Processing completed. Results saved to: {output_csv}")
        return True
        
    except Exception as e:
        import traceback
        print(f"Error in processing: {e}")
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Process Excel file through CyberSource API')
    parser.add_argument('input_excel', help='Path to input Excel file')
    parser.add_argument('output_csv', help='Path to output CSV file')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Use the provided paths
    input_excel = os.path.expanduser(args.input_excel)
    output_csv = os.path.expanduser(args.output_csv)
    
    # Validate input file exists
    if not os.path.exists(input_excel):
        print(f"Error: Input file does not exist: {input_excel}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_csv)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        except Exception as e:
            print(f"Error creating output directory {output_dir}: {e}")
            sys.exit(1)
    
    # Create logs folder if it doesn't exist
    logs_path = os.path.expanduser('~/Documents/Code-Scripts/Work/Contract-FullTime/cybersource-rest-samples-python/Logs')
    if not os.path.exists(logs_path):
        try:
            os.makedirs(logs_path)
        except:
            print(f"Could not create logs directory: {logs_path}")
    
    # Create empty log files to avoid error
    for i in range(1, 11):
        log_file = os.path.join(logs_path, f"cybs.log.{i}")
        try:
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    pass  # Create empty file
        except:
            print(f"Could not create log file: {log_file}")
    
    # Disable logs before starting
    for logger_name in ['CyberSource', 'urllib3', 'requests']:
        python_logging.getLogger(logger_name).setLevel(python_logging.CRITICAL)
    
    # Increase file limit for this process
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"Current file limit: {soft} (soft), {hard} (hard)")
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(8192, hard), hard))
        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"New file limit: {new_soft} (soft), {new_hard} (hard)")
    except Exception as e:
        print(f"Could not change file limit: {e}")
    
    # Display the paths being used
    print(f"Input Excel file: {input_excel}")
    print(f"Output CSV file: {output_csv}")
    
    # Process the file
    print("=== STARTING PROCESSING ===")
    excel_to_csv_processor(input_excel, output_csv)
    print("=== PROCESSING FINISHED ===")