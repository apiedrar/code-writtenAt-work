import os
import uuid
import random
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime

fake = Faker()

load_dotenv()
originacion_key = os.getenv("Originacion_ClaroScore")
hidden_url = os.getenv("originacion_api_url")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# File paths
DATA_XLSX_PATH = os.path.expanduser("~/Downloads/Data_TransactionEvaluations-ClaroScore_Originacion-Prod.xlsx")  # Contains emails, phone_number, and config columns
OUTPUT_EXCEL = os.path.expanduser(f"~/Doownloads/Responses-TransactionEvaluation-{timestamp}.xlsx")

headers = {
    "Content-Type": "application/json",
    "x-api-key": originacion_key
}

def flatten_dict(d, parent_key='', sep='_'):
    """
    Flatten nested dictionary into single level with dot notation keys
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}_{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)

def load_data():
    """
    Load email, phone data, and config columns from single Excel file
    Expected Excel structure:
    | email           | phone_number | score_riesgo | score_fraude | l_profile_idx | ...
    | test@email.com  |  5512345678  |     true     |     false    |     true      | ...
    | test2@email.com |  3387654321  |     false    |     true     |     false     | ...
    """
    try:
        df_data = pd.read_excel(DATA_XLSX_PATH)
        
        # Check if required base columns exist
        required_base_columns = ['email', 'number']
        available_columns = df_data.columns.tolist()
        
        # Handle case where email might be in first column without header
        if 'email' not in available_columns:
            # Try to find email column by position (first column)
            df_data.columns = ['email'] + df_data.columns[1:].tolist()
            available_columns = df_data.columns.tolist()
        
        missing_base_columns = [col for col in required_base_columns if col not in df_data.columns]
        
        if missing_base_columns:
            raise ValueError(f"Missing required base columns in Excel file: {missing_base_columns}\nAvailable columns: {df_data.columns.tolist()}")
        
        # Clean and validate base data
        df_data = df_data.dropna(subset=['email', 'number'])
        
        # Convert phone data to strings and ensure proper formatting
        df_data['number'] = df_data['number'].astype(str).str.strip()
        df_data['email'] = df_data['email'].astype(str).str.strip()
        
        # Identify config columns (all columns except the base required ones)
        config_columns = [col for col in df_data.columns if col not in required_base_columns]
        
        print(f"✅ Loaded {len(df_data)} records from Excel")
        print(f"📋 Found {len(config_columns)} config columns: {config_columns[:5]}{'...' if len(config_columns) > 5 else ''}")
        
        # Convert DataFrame to records, keeping all columns
        records = df_data.to_dict('records')
        
        # Add config_columns info to each record for easy access
        for record in records:
            record['_config_columns'] = config_columns
        
        return records
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        print("📋 Using fallback data...")
        # Fallback to original approach with static config
        fallback_data = [
            {"email": "test1@example.com", "phone_number": "5511111111", "_config_columns": []},
            {"email": "test2@example.com", "phone_number": "3322222222", "_config_columns": []},
            {"email": "test3@example.com", "phone_number": "8133333333", "_config_columns": []},
            {"email": "test4@example.com", "phone_number": "5544444444", "_config_columns": []},
            {"email": "test5@example.com", "phone_number": "3355555555", "_config_columns": []}
        ]
        return fallback_data

def generate_config_from_row(row_data):
    """
    Generate config object from row data based on config columns and their boolean values
    """
    config_columns = row_data.get('_config_columns', [])
    config = {}
    
    for column in config_columns:
        if column in row_data:
            # Handle different possible boolean representations
            value = row_data[column]
            
            # Convert string representations to boolean
            if isinstance(value, str):
                value = value.lower().strip()
                if value in ['true', '1', 'yes', 'y']:
                    config[column] = True
                elif value in ['false', '0', 'no', 'n']:
                    config[column] = False
                else:
                    # Handle any other string values as False by default
                    config[column] = False
            elif isinstance(value, (int, float)):
                # Convert numeric values to boolean (0 = False, anything else = True)
                config[column] = bool(value)
            elif isinstance(value, bool):
                # Already a boolean
                config[column] = value
            else:
                # Default to False for any other type
                config[column] = False
    
    # If no config columns found, fall back to original static config
    if not config:
        config = { k: True for k in [
            "profile_idx", "tch_adaptability_idx", "ctbility_idx", "bill_address_to_full_name_confidence", "bill_address_to_last_name_confidence",
            "bill_city_postal_match", "billing_risk_country", "card_category", "card_type",
            "company_name", "customers_phone_in_billing_location", "dis_description",
            "domain_age", "domain_category", "domain_corporate", "domain_country_code",
            "domain_country_match", "domain_creation_days", "domain_exists", "domain_name",
            "domain_relevant_info", "domain_risk", "domain_risk_level", "ea_advice",
            "ea_reason", "ea_reason_id", "ea_risk_band_id", "ea_score", "email_age",
            "email_creation_days", "email_exists", "email_owner", "email_to_bill_address_confidence",
            "email_to_full_name_confidence", "email_to_ip_confidence", "email_to_last_name_confidence",
            "email_to_phone_confidence", "email_to_ship_address_confidence", "ip_proxy_type",
            "ip_reputation", "last_consultation", "phone_name_match", "phone_owner",
            "phone_status", "phone_to_bill_address_confidence", "phone_to_full_name_confidence",
            "phone_to_last_name_confidence", "phone_to_ship_address_confidence", "ship_city_postal_match",
            "ship_forward", "sm_friends", "source_industry", "status", "u_hits", "title", "cp",
            "rfc", "calle", "ciudad", "estado", "genero", "nombre", "colonia", "materno",
            "paterno", "actividad", "antigplan", "fecha_nac", "domiciliado", "lim_credito",
            "suscripcion", "match_nombre", "estatus_linea", "match_materno", "match_paterno",
            "nivel_recarga", "rango_consumo", "red_contactos", "score_wcredito", "tiposuscripcion",
            "antiguedad_linea", "cadencia_recarga", "nivel_cambio_sim",
            "score_fraude_gen", "score_riesgo_gen", "tendencia_consumo"
        ]}
    
    return config

def generate_payload(email, number, row_data):
    """Generate API payload with email, phone number, and dynamic config"""
    rand_int = lambda: random.randint(1, 10)
    rand_float = lambda: round(random.uniform(10.0, 999.99), 2)
    
    # Generate dynamic config from row data
    dynamic_config = generate_config_from_row(row_data)
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "request": {
            "ipv4": fake.ipv4(),
            "ipv6": fake.ipv6()
        },
        "purchase": {
            "id": str(uuid.uuid4()),
            "created": "2025-04-22T12:48:10-08:00",
            "shipping_address": {
                "street": "Avenida Juarez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "Alcaldia Gustavo A. Madero",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            },
            "phone": {
                "number": number
            },
            "items": [
                {
                    "sku": "12345",
                    "ean_upc": "4011 200296908",
                    "name": "Lentes",
                    "quantity": rand_int(),
                    "unit_amount": rand_float()
                },
                {
                    "sku": "12345",
                    "ean_upc": "4011 200296909",
                    "name": "Petalo 24 pzas",
                    "quantity": rand_int(),
                    "unit_amount": rand_float()
                }
            ],
            "total_items": rand_int(),
            "delivery_date": "2024-11-07T21:20:16-06:00",
            "delivery_service": "UPS",
            "delivery_tracking": "12346535038485",
            "delivery_amount": rand_float(),
            "items_amount": rand_float(),
            "total_amount": rand_float(),
            "device_fingerprint": "1q2w3e4r5t6y7u8i9o0pazsxdcfv"
        },
        "client": {
            "id": str(uuid.uuid4()),
            "name": "John",
            "paternal_surname": "Doe",
            "maternal_surname": "Name",
            "email": email,
            "rfc": "VECJ880326MC",
            "gender": "Hombre",
            "birthdate": "1999-10-23",
            "phone": {  
                "number": number
            },
            "address": {
                "street": "Avenida Juarez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "Alcaldia Gustavo A. Madero",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            },
            "config": dynamic_config  # Use the dynamically generated config
        },
        "merchant": {
            "custom_1": str(uuid.uuid4()),
            "custom_2": "ABCD123456EFGH12",
            "custom_3": number,
            "custom_4": "2001:db8::1",
            "custom_6": str(uuid.uuid4()),
            "custom_15": number,
            "custom_21": "null@cybersource.com",
            "custom_25": "12345-6789",
            "custom_31": "http://www.ejemplo.com"
        },
        "payment_method": {
            "type": "debit card",
            "card_token": str(uuid.uuid4()),
            "bin": "411111",
            "expiration_month": "12",
            "expiration_year": "2030",
            "address": {
                "street": "Avenida Juárez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "N/A",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            },
            "phone": {
                "number": number
            }
        }
    }

def main():
    print("🚀 Starting ClaroScore Originación API Test with Dynamic Config")
    print("=" * 60)
    
    # Load email, phone data, and config columns from same Excel file
    data_records = load_data()
    
    if not data_records:
        print("❌ No data loaded. Exiting...")
        return
    
    results = []
    
    print("\n🔄 Processing API requests...")
    print("-" * 30)
    
    for i, record in enumerate(data_records, 1):
        email = record['email']
        number = record['number']
        
        # Generate payload with dynamic config and make API request
        payload = generate_payload(email, number, record)
        
        # Show config info for first few records
        if i <= 3:
            config_keys = list(payload['client']['config'].keys())
            config_preview = {k: payload['client']['config'][k] for k in list(config_keys)[:5]}
            print(f"[{i:3d}] Config preview: {config_preview}{'...' if len(config_keys) > 5 else ''} (Total: {len(config_keys)} keys)")
        
        try:
            response = requests.post(hidden_url, headers=headers, json=payload)
            status_code = response.status_code
            
            print(f"[{i:3d}/{len(data_records)}] Email: {email[:25]:<25} | Phone: {number} | Status: {status_code}")
            
            # Prepare base row data
            row_data = {
                "test_id": i,
                "email": email,
                "number": number,
                "status_code": status_code,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "config_keys_count": len(payload['client']['config'])
            }
            
            # Add config data to results for reference
            for key, value in payload['client']['config'].items():
                row_data[f"config_{key}"] = value
            
            # Parse and flatten API response
            if status_code == 200:
                try:
                    response_json = response.json()
                    flattened_response = flatten_dict(response_json, sep='_')
                    row_data.update(flattened_response)
                    row_data["api_response_raw"] = response.text
                except json.JSONDecodeError:
                    row_data["api_response_raw"] = response.text
                    row_data["parse_error"] = "Failed to parse JSON response"
            else:
                row_data["api_response_raw"] = response.text
                row_data["error_message"] = f"HTTP {status_code} error"
            
        except Exception as e:
            print(f"[{i:3d}/{len(data_records)}] Email: {email[:25]:<25} | Phone: {number} | ERROR: {str(e)}")
            row_data = {
                "test_id": i,
                "email": email,
                "number": number,
                "status_code": "ERROR",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_message": str(e),
                "api_response_raw": "",
                "config_keys_count": 0
            }
        
        results.append(row_data)
    
    # Create DataFrame and save to Excel
    df_results = pd.DataFrame(results)
    
    # Reorder columns for better readability
    base_columns = ["test_id", "timestamp", "email", "number", "status_code", "config_keys_count"]
    config_columns = [col for col in df_results.columns if col.startswith('config_')]
    other_columns = [col for col in df_results.columns if col not in base_columns + config_columns]
    column_order = base_columns + sorted(config_columns) + sorted(other_columns)
    df_results = df_results.reindex(columns=column_order)
    
    # Save to Excel with formatting
    with pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl') as writer:
        df_results.to_excel(writer, sheet_name='API_Test_Results', index=False)
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['API_Test_Results']
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print("\n" + "=" * 60)
    print("✅ Test completed successfully!")
    print(f"📊 Processed {len(data_records)} records")
    print(f"📁 Results saved to: {OUTPUT_EXCEL}")
    print(f"📋 Total columns in output: {len(df_results.columns)}")
    
    # Show summary statistics
    if len(results) > 0:
        success_count = len([r for r in results if r.get('status_code') == 200])
        error_count = len([r for r in results if r.get('status_code') != 200])
        print(f"✅ Successful requests: {success_count}")
        print(f"❌ Failed requests: {error_count}")
        
        # Show config summary
        if config_columns:
            print(f"🔧 Config columns found: {len(config_columns)}")

if __name__ == "__main__":
    main()