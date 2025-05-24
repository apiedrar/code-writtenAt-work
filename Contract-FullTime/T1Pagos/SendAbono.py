import os
import requests
import csv
import json
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv("api_token")
hidden_url = os.getenv("APISendAbono_URL")
# File containing IDs and other data
input_csv_file = os.path.expanduser('~/Downloads/SendAbono_20250425.csv')
# File containing responses
output_csv_file = os.path.expanduser('~/Downloads/SendAbono_20250425_Responses.csv')

# Endpoint to which POST requests will be directed
url = hidden_url
# Headers, including access token
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

# Values that should not be modified
FIXED_VALUES = {
    "empresa": "CLARO_PAGOS",
    "tipoPago": "1",  # Kept as string as per example
    "tipoCuentaOrdenante": 40,
    "tipoCuentaBeneficiario": 40,
    "institucionBeneficiaria": 90646
}

# Read data from CSV file
with open(input_csv_file, mode='r') as file:
    reader = csv.DictReader(file)
    rows = list(reader)
    
    # Calculate total number of rows
    total_rows = len(rows)
    
    # List storing all API responses and errors
    all_data = []
    
    # Process each row in CSV file
    for index, row in enumerate(rows):
        # Create payload from row data
        payload = {}
        
        # Convert types appropriately
        for key, value in row.items():
            if key == 'uuid':  # Skip uuid as it's not part of the payload
                continue
                
            # Handle various data types - preserving strings that should be strings
            if key in ['id', 'referenciaNumerica', 'tipoCuentaOrdenante',
                      'institucionOrdenante', 'tipoCuentaBeneficiario', 'institucionBeneficiaria']:
                try:
                    payload[key] = int(value) if value else 0
                except ValueError:
                    payload[key] = 0
            elif key == 'monto':
                try:
                    payload[key] = float(value) if value else 0.0
                except ValueError:
                    payload[key] = 0.0
            # Preserve these fields as strings to maintain leading zeros
            elif key in ['claveRastreo', 'conceptoPago', 'fechaOperacion', 'cuentaOrdenante', 
                        'rfcCurpOrdenante', 'cuentaBeneficiario']:
                payload[key] = str(value)
            else:
                payload[key] = value
        
        # Apply fixed values, overriding any from the CSV
        for key, value in FIXED_VALUES.items():
            payload[key] = value
            
        # Get the ID for the request URL
        id_value = row.get('uuid', '')
        request_url = f"{url}"
        
        print(f"[{index+1}/{total_rows}] Sending to URL: {request_url}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        try:
            response = requests.post(request_url, headers=headers, json=payload)
            response_data = response.json() if response.headers.get('Content-Type') == 'application/json' else {}
            
            if response.status_code == 200:
                response_data['id'] = id_value  # Add ID to response
                response_data['error'] = ''  # Empty column if no error is returned
                all_data.append(response_data)
            else:
                error_message = response_data.get('error', response.text)
                print(f'Error on ID {id_value}: {response.status_code} - {error_message}')
                all_data.append({'id': id_value, 'error': error_message})
        except requests.exceptions.RequestException as e:
            print(f"Connection error on ID {id_value}: {e}")
            all_data.append({'id': id_value, 'error': str(e)})
    
    # Save all responses to CSV file
    if all_data:
        with open(output_csv_file, mode='w', newline='') as file:
            # Determine all possible keys across all responses
            all_keys = set()
            for item in all_data:
                all_keys.update(item.keys())
            
            writer = csv.DictWriter(file, fieldnames=list(all_keys))
            writer.writeheader()
            writer.writerows(all_data)
        
        print(f"Successfully wrote {len(all_data)} responses to {output_csv_file}")
    else:
        print("No data fetched to write CSV file.")