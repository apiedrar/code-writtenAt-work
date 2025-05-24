import os
import time
import uuid
import random
import logging
import pandas as pd
import json
import csv
from dotenv import load_dotenv
from datetime import datetime
from threading import Lock
from locust import HttpUser, task, between, events
from gevent.lock import Semaphore
from zoneinfo import ZoneInfo

load_dotenv()
sears_token = os.getenv("Sears_Token")
# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base path and files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_PATH = os.path.expanduser("~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests")
# Making file paths more flexible - check if files exist
EMAILS_FILE = os.path.join(BASE_PATH, "emails.xlsx")
PHONES_FILE = os.path.join(BASE_PATH, "telefonos.xlsx")  # Keep Spanish filename
TOKENS_FILE = os.path.join(BASE_PATH, "tokens.xlsx")
BINES_FILE = os.path.join(BASE_PATH, "bines.xlsx")
CSV_ERROR_PATH = os.path.join(BASE_PATH, f"errors_logging/errors_sears_{timestamp}.csv")

# Check for directory existence and create if needed
os.makedirs(os.path.dirname(CSV_ERROR_PATH), exist_ok=True)

# Locks
counter_lock = Semaphore()
csv_lock = Lock()

# Transaction limit
MAX_REQUESTS = 10000
request_counter = 0

def increment_counter():
    global request_counter
    with counter_lock:
        request_counter += 1
        return request_counter

# Lists loading from Excel with better error handling
def load_excel_list(file_path, column=0):
    try:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}, returning empty list")
            return []
        df = pd.read_excel(file_path, engine='openpyxl')
        return df.iloc[:, column].dropna().astype(str).str.strip().tolist()
    except Exception as e:
        logger.error(f"Error loading Excel file {file_path}: {e}")
        return []  # Return empty list on error instead of failing

# Pre-load data with error handling
try:
    DATA_CACHE = {
        "velocity_test": {
            "emails": load_excel_list(EMAILS_FILE),
            "phones": load_excel_list(PHONES_FILE),
            "tokens": load_excel_list(TOKENS_FILE),
            "bines": load_excel_list(BINES_FILE)
        }
    }
    # Add fallback data if files were empty
    if not DATA_CACHE["velocity_test"]["emails"]:
        DATA_CACHE["velocity_test"]["emails"] = [f"fallback_{i}@example.com" for i in range(10)]
    if not DATA_CACHE["velocity_test"]["phones"]:
        DATA_CACHE["velocity_test"]["phones"] = [f"{random.randint(10000000, 99999999)}" for i in range(10)]
    if not DATA_CACHE["velocity_test"]["tokens"]:
        DATA_CACHE["velocity_test"]["tokens"] = [str(uuid.uuid4()) for i in range(10)]
    if not DATA_CACHE["velocity_test"]["bines"]:
        DATA_CACHE["velocity_test"]["bines"] = ["411111", "451234", "371234"]
except Exception as e:
    logger.error(f"Error initializing data cache: {e}")
    # Provide fallback data
    DATA_CACHE = {
        "velocity_test": {
            "emails": [f"fallback_{i}@example.com" for i in range(10)],
            "phones": [f"{random.randint(10000000, 99999999)}" for i in range(10)],
            "tokens": [str(uuid.uuid4()) for i in range(10)],
            "bines": ["411111", "451234", "371234"]
        }
    }

SEARS_PROFILES = {
    "PAGO TARJETA SEARS": lambda payload: payload["purchase"]["items"][0].update({"ean_upc": "1041064"}),  # Keep Spanish profile name
    "CREDITO SEARS": lambda payload: payload["merchant"].update({"custom_32": "1"}),  # Keep Spanish profile name
    "ECOMMERCE SEARS": lambda payload: payload["purchase"].update({"total_amount": round(random.uniform(500, 10000), 2)})  # Keep Spanish profile name
}

def generate_payload(profile, email, phone, token, bin_value=None):
    try:
        current_datetime = datetime.now(ZoneInfo("America/Mexico_City"))
        formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S%z")
        # Fix timezone format to include colon
        if ":" not in formatted_datetime[-5:]:
            formatted_datetime = formatted_datetime[:-2] + ':' + formatted_datetime[-2:]
            
        payload = {
            "transaction_id": str(uuid.uuid4()),
            "request": {
                "ipv4": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
                "ipv6": "2001:db8::1"
            },
            "purchase": {
                "id": str(uuid.uuid4()),
                "created": formatted_datetime,
                "shipping_address": {
                    "street": "Avenida Juarez",  # Keep Spanish street name
                    "external_number": "213",
                    "internal_number": "1A",
                    "town": "Roma Norte",  # Keep Spanish location
                    "city": "Alcaldia Gustavo A. Madero",  # Keep Spanish location
                    "state": "MX",
                    "country": "MX",
                    "zip_code": "09960"
                },
                "phone": {
                    "number": str(phone)
                },
                "items": [
                    {
                        "sku": "12345",
                        "ean_upc": "4011 200296908",
                        "name": "Lentes",  # Keep Spanish product name
                        "quantity": random.randint(1, 5),
                        "unit_amount": round(random.uniform(100, 500), 2)
                    }
                ],
                "total_items": 1,
                "delivery_date": "2024-11-07T21:20:16-06:00",
                "delivery_service": "UPS",
                "delivery_tracking": str(random.randint(10000000,99999999)),
                "delivery_amount": round(random.uniform(50, 300), 2),
                "items_amount": round(random.uniform(100, 1000), 2),
                "total_amount": 0,  # Will be calculated or updated by profile
                "device_fingerprint": "1q2w3e4r5t6y7u8i9o0pazsxdcfv"
            },
            "client": {
                "id": str(uuid.uuid4()),
                "name": "Clara Luz",  # Keep Spanish name
                "paternal_surname": "Aguilar",  # Keep Spanish surname
                "maternal_surname": "Hernandez",  # Keep Spanish surname, removed accent
                "email": email,
                "rfc": "VECJ880326MC",  # Keep Mexican RFC format
                "gender": "Hombre",  # Keep Spanish gender
                "birthdate": "1999-10-23",
                "phone": {
                    "number": str(phone)
                },
                "address": {
                    "street": "Avenida Juarez",  # Keep Spanish street name
                    "external_number": "213",
                    "internal_number": "1A",
                    "town": "Roma Norte",  # Keep Spanish location
                    "city": "Alcaldia Gustavo A. Madero",  # Keep Spanish location
                    "state": "MX",
                    "country": "MX",
                    "zip_code": "09960"
                },
                "config": {}
            },
            "merchant": {
                "custom_1": str(uuid.uuid4()),
                "custom_2": "ABCD123456EFGH12",
                "custom_3": phone[-7:] if len(phone) >= 7 else phone,
                "custom_4": "2001:db8::1",
                "custom_6": str(uuid.uuid4()),
                "custom_15": phone[-7:] if len(phone) >= 7 else phone,
                "custom_21": email,
                "custom_25": "12345-6789",
                "custom_31": "http://www.ejemplo.com"  # Keep Spanish example URL
            },
            "payment_method": {
                "type": "debit card",
                "card_token": token,
                "bin": bin_value or "411111",
                "expiration_month": "12",
                "expiration_year": "2030",
                "address": {
                    "street": "Avenida Juarez",  # Keep Spanish street name
                    "external_number": "213",
                    "internal_number": "1A",
                    "town": "Roma Norte",  # Keep Spanish location
                    "city": "N/A",
                    "state": "MX",
                    "country": "MX",
                    "zip_code": "09960"
                },
                "phone": {
                    "number": str(phone)
                }
            }
        }

        # Apply profile customizations
        if profile in SEARS_PROFILES:
            SEARS_PROFILES[profile](payload)
        
        # Ensure total_amount is set
        if payload["purchase"]["total_amount"] == 0:
            # Calculate total if not set by profile
            items_total = sum(item["quantity"] * item["unit_amount"] for item in payload["purchase"]["items"])
            payload["purchase"]["total_amount"] = round(items_total + payload["purchase"]["delivery_amount"], 2)

        return payload
    except Exception as e:
        logger.error(f"Error generating payload: {e}")
        # Return a minimal valid payload
        return {
            "transaction_id": str(uuid.uuid4()),
            "request": {"ipv4": "192.168.1.1"},
            "purchase": {"total_amount": 100},
            "client": {"email": email},
            "payment_method": {"card_token": token}
        }

class VelocityTestUser(HttpUser):
    wait_time = between(0.1, 0.5)
    # host will be set via command line parameter

    def _get_random_profile(self):
        return random.choice(list(SEARS_PROFILES.keys()))

    def _get_headers(self):
        return {
            "x-api-key": f"{sears_token}",
            "Content-Type": "application/json"
        }

    def _send_request(self, payload, name):
        try:
            # Check if we've reached the limit
            current_count = increment_counter()
            if current_count > MAX_REQUESTS:
                logger.info(f"Reached max requests limit ({MAX_REQUESTS}), stopping test")
                # Add a small delay before stopping to allow in-flight requests to complete
                time.sleep(0.5)
                try:
                    # Try the safer method first
                    self.environment.runner.quit()
                except Exception as e:
                    logger.error(f"Error during quit: {e}")
                    # Fallback to older methods if available
                    try:
                        self.environment.stopped = True
                    except:
                        pass
                return

            with self.client.post("/engine/transactions", json=payload, headers=self._get_headers(), name=name, catch_response=True) as response:
                status_code = response.status_code
                logger.debug(f"[{name}] Status: {status_code}")

                if status_code != 200:
                    # Safely get response text
                    try:
                        error_msg = response.text.strip()
                    except Exception:
                        error_msg = "Could not read response text"

                    timestamp = datetime.now().isoformat()
                    
                    # Convert payload to JSON safely
                    try:
                        payload_json = json.dumps(payload, ensure_ascii=False)
                    except Exception:
                        payload_json = "Could not serialize payload"

                    # Write to CSV with error handling
                    try:
                        with csv_lock:
                            file_exists = os.path.isfile(CSV_ERROR_PATH)
                            with open(CSV_ERROR_PATH, "a", newline='', encoding="utf-8") as f:
                                writer = csv.writer(f)
                                if not file_exists:
                                    writer.writerow(["timestamp", "status_code", "error_message", "payload"])
                                writer.writerow([timestamp, status_code, error_msg, payload_json])
                    except Exception as e:
                        logger.error(f"Error writing to CSV: {e}")

                    response.failure(f"Status {status_code}: {error_msg}")
                else:
                    response.success()
        except Exception as e:
            logger.error(f"Error in _send_request: {e}")

    @task(3)
    def test_token_velocity(self):
        try:
            if not DATA_CACHE["velocity_test"]["tokens"]:
                logger.warning("No tokens available for token velocity test")
                return
                
            profile = self._get_random_profile()
            token = random.choice(DATA_CACHE["velocity_test"]["tokens"])
            email = f"velocity_{random.randint(1000, 9999)}@example.com"
            phone = f"{random.randint(10000000, 99999999)}"
            bin_value = random.choice(DATA_CACHE["velocity_test"]["bines"]) if DATA_CACHE["velocity_test"]["bines"] else "411111"
            
            payload = generate_payload(profile, email, phone, token, bin_value)
            self._send_request(payload, "Velocity-Token-SEARS")
        except Exception as e:
            logger.error(f"Error in test_token_velocity: {e}")

    @task(2)
    def test_email_velocity(self):
        try:
            if not DATA_CACHE["velocity_test"]["emails"]:
                logger.warning("No emails available for email velocity test")
                return
                
            profile = self._get_random_profile()
            email = random.choice(DATA_CACHE["velocity_test"]["emails"])
            token = str(uuid.uuid4())
            phone = f"{random.randint(10000000, 99999999)}"
            bin_value = random.choice(DATA_CACHE["velocity_test"]["bines"]) if DATA_CACHE["velocity_test"]["bines"] else "411111"
            
            payload = generate_payload(profile, email, phone, token, bin_value)
            self._send_request(payload, "Velocity-Email-SEARS")
        except Exception as e:
            logger.error(f"Error in test_email_velocity: {e}")

    @task(2)
    def test_phone_velocity(self):
        try:
            if not DATA_CACHE["velocity_test"]["phones"]:
                logger.warning("No phones available for phone velocity test")
                return
                
            profile = self._get_random_profile()
            phone = random.choice(DATA_CACHE["velocity_test"]["phones"])
            email = f"velocity_{random.randint(1000, 9999)}@example.com"
            token = str(uuid.uuid4())
            bin_value = random.choice(DATA_CACHE["velocity_test"]["bines"]) if DATA_CACHE["velocity_test"]["bines"] else "411111"
            
            payload = generate_payload(profile, email, phone, token, bin_value)
            self._send_request(payload, "Velocity-Phone-SEARS")
        except Exception as e:
            logger.error(f"Error in test_phone_velocity: {e}")

    @task(1)
    def test_velocity_rules(self):
        try:
            profile = self._get_random_profile()
            email = f"rules_{random.randint(1000, 9999)}@example.com"
            phone = f"{random.randint(10000000, 99999999)}"
            token = str(uuid.uuid4())
            bin_value = random.choice(DATA_CACHE["velocity_test"]["bines"]) if DATA_CACHE["velocity_test"]["bines"] else "411111"
            
            payload = generate_payload(profile, email, phone, token, bin_value)
            self._send_request(payload, "Velocity-Rules-SEARS")
        except Exception as e:
            logger.error(f"Error in test_velocity_rules: {e}")

# Global test event handlers
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logger.info("✅ Tests for Sears in ClaroScore API: Initializing")
    # Reset counter at start
    global request_counter
    request_counter = 0

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    logger.info("🛑 Tests for Sears in ClaroScore API: Terminated")
    # Calculate stats safely
    try:
        total = environment.stats.total.num_requests
        fail = environment.stats.total.fail_ratio
        logger.info(f"Total requests: {total}")
        logger.info(f"Failure rate: {fail:.2%}")
    except Exception as e:
        logger.error(f"Error calculating final stats: {e}")