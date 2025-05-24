import os
import requests
import csv
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv('api_token')
hidden_url = os.getenv('APISync_URL')
# File containing IDs
input_csv_file = os.path.expanduser('~/Downloads/Query_RyPTelcel_20250521.csv')  # Verify a column named 'uuid' exists in your file
# File containing responses
output_csv_file = os.path.expanduser('~/Downloads/SyncResponses_RyPTelcel_20250521.csv')

# Headers, including access token
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

# Read IDs from CSV file
with open(input_csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    rows = list(reader)
    
    # Calculate total number of IDs
    total_ids = len(rows)
    
    # List storing all API responses and errors
    all_data = []
    
    # Process each row in CSV file (assuming a 'id' column exists)
    for index, row in enumerate(rows):
        id_value = row['uuid']
        url = f"{hidden_url}{id_value}"
        
        print(f"[{index+1}/{total_ids}] Requesting URL: {url}")
        
        try:
            response = requests.patch(url, headers=headers)
            response_data = response.json() if response.headers.get('Content-Type') == 'application/json' else {}
            
            if response.status_code == 200:
                response_data['id'] = id_value  # Add ID to response for better understading
                response_data['error'] = ''  # Empty column if no error is returned
                all_data.append(response_data)
            else:
                error_message = response_data.get('error', response.text)  # Attempts to extract error from JSON, otherwise, full text is logged
                print(f'Error on ID {id_value}: {response.status_code} - {error_message}')
                all_data.append({'id': id_value, 'error': error_message})
        except requests.exceptions.RequestException as e:
            print(f"Connection error on ID {id_value}: {e}")
            all_data.append({'id': id_value, 'error': str(e)})
    
    # Save all responses to CSV file
    if all_data:
        with open(output_csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Write headers (columns)
            headers = list(all_data[0].keys())
            writer.writerow(headers)
            
            # Write values (rows)
            for item in all_data:
                writer.writerow([item.get(h, '') for h in headers])
    else:
        print("No data fetched to write to CSV file.")
