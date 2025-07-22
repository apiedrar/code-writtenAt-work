import requests
import json
import uuid
import random
import string
import argparse
import csv
import datetime
import ipaddress
from time import sleep
from typing import Dict, Any, List, Optional


def generate_uuid() -> str:
    """Generate a random UUID."""
    return str(uuid.uuid4())


def generate_ipv4() -> str:
    """Generate a random IPv4 address."""
    return ".".join(str(random.randint(1, 255)) for _ in range(4))


def generate_ipv6() -> str:
    """Generate a random IPv6 address."""
    addr = ipaddress.IPv6Address(random.randint(0, 2**128-1))
    return str(addr)


def generate_device_fingerprint(length: int = 30) -> str:
    """Generate a random device fingerprint."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def generate_card_token() -> str:
    """Generate a random card token."""
    return generate_uuid()


def generate_bin() -> str:
    """Generate a random BIN (Bank Identification Number)."""
    return ''.join(random.choices(string.digits, k=6))


def generate_tracking_number() -> str:
    """Generate a random tracking number."""
    return ''.join(random.choices(string.digits, k=14))


def generate_phone_number() -> Dict[str, str]:
    """Generate a random phone number."""
    return {
        "country_code": str(random.randint(1, 999)),
        "area_code": str(random.randint(100, 999)),
        "number": ''.join(random.choices(string.digits, k=7))
    }


def generate_timestamp(future: bool = False) -> str:
    """Generate a random timestamp."""
    now = datetime.datetime.now()
    if future:
        # Generate a date up to 30 days in the future
        delta = datetime.timedelta(days=random.randint(1, 30))
        date = now + delta
    else:
        # Generate a date up to 30 days in the past
        delta = datetime.timedelta(days=random.randint(0, 30))
        date = now - delta
    
    # Format with timezone offset
    timezone_offset = random.choice(["-08:00", "-06:00", "-05:00", "+00:00"])
    return date.strftime(f"%Y-%m-%dT%H:%M:%S{timezone_offset}")


def load_custom_data(csv_file_path: str) -> List[Dict[str, Any]]:
    """Load custom data from a CSV file."""
    data_rows = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data_rows.append(row)
        print(f"Successfully loaded {len(data_rows)} rows from {csv_file_path}")
        return data_rows
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return []


def build_payload(custom_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build a payload based on the template, with random data and custom data if provided."""
    
    # Start with a basic payload template
    payload = {
        "transaction_id": generate_uuid(),
        "commerce_id": 2,  # This should be overridden by custom data
        "request": {
            "ipv4": generate_ipv4(),
            "ipv6": generate_ipv6()
        },
        "purchase": {
            "id": generate_uuid(),
            "created": generate_timestamp(),
            "shipping_address": {
                "street": "Avenida Juárez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "Alcaldía Gustavo A. Madero",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            },
            "items": [
                {
                    "sku": "12345",
                    "ean_upc": "4011 200296908",  # This should be overridden by custom data
                    "name": "Lentes",
                    "quantity": random.randint(1, 1000),
                    "unit_amount": round(random.uniform(100, 20000), 2)
                }
            ],
            "total_items": random.randint(1, 1000),
            "delivery_date": generate_timestamp(future=True),
            "delivery_service": random.choice(["UPS", "DHL", "FedEx"]),
            "delivery_tracking": generate_tracking_number(),
            "items_amount": None,  # Will be calculated later
            "delivery_amount": round(random.uniform(50, 500), 2),
            "total_amount": None,  # Will be calculated later
            "device_fingerprint": generate_device_fingerprint()
        },
        "client": {
            "id": generate_uuid(),  # This should be overridden by custom data
            "name": "Clara Luz",
            "paternal_surname": "Aguilar",
            "maternal_surname": "Hernández",
            "email": f"{generate_device_fingerprint(8)}@gmail.com",
            "phone": generate_phone_number(),
            "address": {
                "street": "Avenida Juárez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "Alcaldía Gustavo A. Madero",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            }
        },
        "payment_method": {
            "expiration_month": str(random.randint(1, 12)).zfill(2),
            "expiration_year": str(random.randint(2024, 2030)),
            "card_token": generate_card_token(),
            "bin": generate_bin(),
            "type": random.choice(["debit card", "credit card"]),
            "address": {
                "street": "Avenida Juárez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
                "city": "N/A",
                "state": "MX",
                "country": "MX",
                "zip_code": "09960"
            }
        }
    }
    
    # Calculate items_amount based on the first item
    item = payload["purchase"]["items"][0]
    items_amount = item["quantity"] * item["unit_amount"]
    payload["purchase"]["items_amount"] = round(items_amount, 2)
    
    # Calculate total_amount
    delivery_amount = payload["purchase"]["delivery_amount"]
    payload["purchase"]["total_amount"] = round(items_amount + delivery_amount, 2)
    
    # Override with custom data if provided
    if custom_data:
        # Commerce ID
        if "commerce_id" in custom_data:
            payload["commerce_id"] = int(custom_data["commerce_id"])
            
        # EAN/UPC
        if "ean_upc" in custom_data:
            payload["purchase"]["items"][0]["ean_upc"] = custom_data["ean_upc"]
            
        # Client ID
        if "client_id" in custom_data:
            payload["client"]["id"] = custom_data["client_id"]
            
        # Override any other fields if they exist in custom_data
        for key, value in custom_data.items():
            # Handle nested fields using dot notation (e.g., "client.name")
            if "." in key:
                parts = key.split(".")
                current = payload
                for part in parts[:-1]:
                    if part in current:
                        current = current[part]
                    else:
                        break
                else:
                    last_part = parts[-1]
                    if last_part in current:
                        # Try to convert to the same type as the template
                        original_type = type(current[last_part])
                        try:
                            if original_type == int:
                                current[last_part] = int(value)
                            elif original_type == float:
                                current[last_part] = float(value)
                            elif original_type == bool:
                                current[last_part] = value.lower() in ('true', 'yes', '1', 't', 'y')
                            else:
                                current[last_part] = value
                        except (ValueError, TypeError):
                            current[last_part] = value
    
    return payload


def send_request(url: str, payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Send a POST request with the payload and return the response."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        return {
            "status_code": response.status_code,
            "response": response.text
        }
    except Exception as e:
        return {
            "status_code": None,
            "response": f"Error: {str(e)}"
        }


def main():
    parser = argparse.ArgumentParser(description="Send payload requests.")
    parser.add_argument("--url", required=True, help="URL to send the requests to")
    parser.add_argument("--api-key", required=True, help="API key for authentication")
    parser.add_argument("--num-requests", type=int, default=1, help="Number of requests to send")
    parser.add_argument("--csv-file", help="Path to CSV file with custom data")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds")
    parser.add_argument("--output", help="Output file for responses")
    
    args = parser.parse_args()
    
    # Load custom data if provided
    custom_data_rows = []
    if args.csv_file:
        custom_data_rows = load_custom_data(args.csv_file)
    
    # Track responses
    responses = []
    
    # Send requests
    for i in range(args.num_requests):
        print(f"\nSending request {i+1}/{args.num_requests}...")
        
        # Get custom data for this request
        custom_data = None
        if custom_data_rows:
            # Cycle through the rows if there are fewer rows than requests
            row_index = i % len(custom_data_rows)
            custom_data = custom_data_rows[row_index]
        
        # Build and send payload
        payload = build_payload(custom_data)
        response = send_request(args.url, payload, args.api_key)
        
        # Print results
        print(f"Status code: {response['status_code']}")
        print(f"Response: {response['response'][:100]}..." if len(response['response']) > 100 else f"Response: {response['response']}")
        
        # Store response
        responses.append({
            "request_number": i+1,
            "transaction_id": payload["transaction_id"],
            "status_code": response["status_code"],
            "response": response["response"]
        })
        
        # Add delay between requests (except for the last one)
        if i < args.num_requests - 1:
            sleep(args.delay)
    
    # Save responses if output file is provided
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(responses, f, indent=2)
            print(f"\nResponses saved to {args.output}")
        except Exception as e:
            print(f"\nError saving responses: {e}")
    
    print(f"\nCompleted {args.num_requests} requests.")


if __name__ == "__main__":
    main()
