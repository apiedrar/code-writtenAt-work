#!/usr/bin/env python3
"""
API Validation Script
Sends GET requests to validate emails/phone numbers against multiple lists
and outputs results to an Excel file.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
import time
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

def main():
    # Configuration
    API_BASE_URL = os.getenv('dev_VerifyItem_URL')  # Set this in your .env file
    INPUT_FILE = os.path.expanduser('~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/ClaroScore-Originacion/Tests-20250603.xlsx')  # Modify path as needed
    OUTPUT_FILE = os.path.expanduser('~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/ClaroScore-Originacion/Verify-20250603.xlsx')  # Modify path as needed
    
    # List of IDs to iterate through - Parse from environment variable
    id_list_str = os.getenv('id_list', '')
    if id_list_str:
        # Split by comma and strip whitespace
        ID_LIST = [item.strip() for item in id_list_str.split(',')]
    else:
        # Fallback list if env var not set
        ID_LIST = []
    
    # Request configuration
    REQUEST_DELAY = 0.05  # Delay between requests in seconds
    TIMEOUT = 30  # Request timeout in seconds
    
    print("Starting API validation process...")
    print(f"API Base URL: {API_BASE_URL}")
    print(f"ID List: {ID_LIST}")
    print(f"Number of lists to check: {len(ID_LIST)}")
    
    if not API_BASE_URL:
        raise ValueError("API_BASE_URL not found in environment variables. Please check your .env file.")
    
    try:
        # Read Excel file
        print(f"Reading input file: {INPUT_FILE}")
        df = pd.read_excel(INPUT_FILE)
        
        # Validate required columns
        required_columns = ['area_code', 'phone_number', 'email']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Create phone numbers by concatenating area_code and phone_number
        df['full_phone'] = df['area_code'].astype(str) + df['phone_number'].astype(str)
        
        print(f"Processing {len(df)} rows with {len(ID_LIST)} lists...")
        
        # Initialize result columns for each list ID
        for list_id in ID_LIST:
            df[f'{list_id}_email'] = 0  # Default to 0 (false)
            df[f'{list_id}_phone'] = 0  # Default to 0 (false)
        
        # Process each row
        for index, row in df.iterrows():
            email = row['email']
            phone = row['full_phone']
            
            print(f"Processing row {index + 1}/{len(df)}")
            
            # Process email validation for each list
            if pd.notna(email) and email.strip():
                for list_id in ID_LIST:
                    result = validate_value(API_BASE_URL, list_id, email.strip(), TIMEOUT)
                    df.at[index, f'{list_id}_email'] = 1 if result else 0
                    time.sleep(REQUEST_DELAY)
            
            # Process phone validation for each list
            if pd.notna(phone) and phone.strip():
                for list_id in ID_LIST:
                    result = validate_value(API_BASE_URL, list_id, phone.strip(), TIMEOUT)
                    df.at[index, f'{list_id}_phone'] = 1 if result else 0
                    time.sleep(REQUEST_DELAY)
        
        # Save results to new Excel file
        print(f"Saving results to: {OUTPUT_FILE}")
        df.to_excel(OUTPUT_FILE, index=False)
        
        print("Process completed successfully!")
        print(f"Results saved to: {OUTPUT_FILE}")
        
        # Display summary
        print("\nSummary:")
        for list_id in ID_LIST:
            email_matches = df[f'{list_id}_email'].sum()
            phone_matches = df[f'{list_id}_phone'].sum()
            print(f"  {list_id}: {email_matches} email matches, {phone_matches} phone matches")
            
    except FileNotFoundError:
        print(f"Error: Input file not found at {INPUT_FILE}")
        print("Please check the file path and ensure the file exists.")
    except Exception as e:
        print(f"Error: {str(e)}")


def validate_value(base_url, list_id, value, timeout=30):
    """
    Send GET request to validate a value against a specific list.
    
    Args:
        base_url (str): Base API URL
        list_id (str): List identifier
        value (str): Value to validate (email or phone)
        timeout (int): Request timeout in seconds
    
    Returns:
        bool: True if found, False otherwise
    """
    try:
        # Construct the URL manually instead of using urljoin
        # Ensure base_url ends with proper format
        if not base_url.endswith('/'):
            base_url = base_url + '/'
        
        # Build the complete URL
        url = f"{base_url}v1/list/{list_id}/items/{value}/verify"
        
        # Set headers similar to Postman
        headers = {
            'User-Agent': 'Python-Script/1.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        }
        
        # Debug: Print the constructed URL
        print(f"    Requesting: {url}")
        
        # Make GET request
        response = requests.get(url, headers=headers, timeout=timeout)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            # Extract the 'found' boolean value
            return bool(data.get('found', False))
        else:
            print(f"  Warning: API request failed for {list_id}/{value} - Status: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"  Warning: Request timeout for {list_id}/{value}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Request error for {list_id}/{value}: {str(e)}")
        return False
    except ValueError as e:
        print(f"  Warning: JSON parsing error for {list_id}/{value}: {str(e)}")
        return False
    except Exception as e:
        print(f"  Warning: Unexpected error for {list_id}/{value}: {str(e)}")
        return False


if __name__ == "__main__":
    main()