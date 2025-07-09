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
claropay_token = os.getenv("ClaroPay_Token")
# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base path and files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_PATH = os.path.expanduser("~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests")
EMAILS_FILE = os.path.join(BASE_PATH, "emails.xlsx")
PHONES_FILE = os.path.join(BASE_PATH, "telefonos.xlsx")
TOKENS_FILE = os.path.join(BASE_PATH, "tokens.xlsx")
BINS_FILE = os.path.join(BASE_PATH, "bines.xlsx")
CSV_ERROR_PATH = os.path.join(BASE_PATH, f"errors_logging/errores_claropay_{timestamp}.csv")

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

# Excel list loading
def load_excel_list(file_path, column=0):
    df = pd.read_excel(file_path, engine='openpyxl')
    return df.iloc[:, column].dropna().astype(str).str.strip().tolist()

DATA_CACHE = {
    "velocity_test": {
        "emails": load_excel_list(EMAILS_FILE),
        "phones": load_excel_list(PHONES_FILE),
        "tokens": load_excel_list(TOKENS_FILE),
        "bins": load_excel_list(BINS_FILE)
    }
}

CLAROPAY_PROFILES = {
    "CLARO_PAY": lambda payload: payload["purchase"].update({"total_amount": round(random.uniform(500, 10000), 2)})
}

def generate_payload(profile, email, phone, token, bin_value=None):
    payload = {
        "transaction_id": str(uuid.uuid4()),
        "request": {
            "ipv4": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
            "ipv6": "2001:db8::1"
        },
        "purchase": {
            "id": str(uuid.uuid4()),
            "created": datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%dT%H:%M:%S%z")[:-2] + ':' + datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%dT%H:%M:%S%z")[-2:],
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
                "number": str(phone)
            },
            "items": [
                {
                    "sku": "12345",
                    "ean_upc": "4011 200296908",
                    "name": "Lentes",
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
            "total_amount": 0,
            "device_fingerprint": "1q2w3e4r5t6y7u8i9o0pazsxdcfv"
        },
        "client": {
            "id": str(uuid.uuid4()),
            "name": "Clara Luz",
            "paternal_surname": "Aguilar",
            "maternal_surname": "Hernandez",
            "email": email,
            "rfc": "VECJ880326MC",
            "gender": "Hombre",
            "birthdate": "1999-10-23",
            "phone": {
                "number": str(phone)
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
            "config": {}
        },
        "merchant": {
            "custom_1": str(uuid.uuid4()),
            "custom_2": "ABCD123456EFGH12",
            "custom_3": phone[-7:],
            "custom_4": phone[-7:],
            "custom_6": str(uuid.uuid4()),
            "custom_15": phone[-7:],
            "custom_21": email,
            "custom_25": "12345-6789",
            "custom_31": "http://www.ejemplo.com"
        },
        "payment_method": {
            "type": "debit card",
            "card_token": token,
            "bin": bin_value or "411111",
            "expiration_month": "12",
            "expiration_year": "2030",
            "address": {
                "street": "Avenida Juarez",
                "external_number": "213",
                "internal_number": "1A",
                "town": "Roma Norte",
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

    if profile in CLAROPAY_PROFILES:
        CLAROPAY_PROFILES[profile](payload)

    return payload

class VelocityTestUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "https://cv6r41wpx8.execute-api.us-east-1.amazonaws.com/dev/v1"

    def _get_random_profile(self):
        return random.choice(list(CLAROPAY_PROFILES.keys()))

    def _get_headers(self):
        return {
            "x-api-key": f"{claropay_token}",
            "Content-Type": "application/json"
        }

    def _send_request(self, payload, name):
        if increment_counter() > MAX_REQUESTS:
            self.environment.runner.quit()
            return

        with self.client.post("/engine/transactions", json=payload, headers=self._get_headers(), name=name, catch_response=True) as response:
            logger.debug(f"[{name}] Status: {response.status_code}")

            if response.status_code != 200:
                timestamp = datetime.now().isoformat()
                status_code = response.status_code
                error_msg = response.text.strip()
                payload_json = json.dumps(payload, ensure_ascii=False)

                with csv_lock:
                    file_exists = os.path.isfile(CSV_ERROR_PATH)
                    with open(CSV_ERROR_PATH, "a", newline='', encoding="utf-8") as f:
                        writer = csv.writer(f)
                        if not file_exists:
                            writer.writerow(["timestamp", "status_code", "error_message", "payload"])
                        writer.writerow([timestamp, status_code, error_msg, payload_json])

                response.failure(f"Status {status_code}: {error_msg}")
            else:
                response.success()

    @task(3)
    def test_token_velocity(self):
        if not DATA_CACHE["velocity_test"]["tokens"]:
            return
        profile = self._get_random_profile()
        token = random.choice(DATA_CACHE["velocity_test"]["tokens"])
        email = f"velocity_{random.randint(1000, 9999)}@example.com"
        phone = f"{random.randint(10000000, 99999999)}"
        bin_value = random.choice(DATA_CACHE["velocity_test"]["bins"]) if DATA_CACHE["velocity_test"]["bins"] else "411111"
        payload = generate_payload(profile, email, phone, token, bin_value)
        self._send_request(payload, "Velocity-Token-CLAROPAY")

    @task(2)
    def test_email_velocity(self):
        if not DATA_CACHE["velocity_test"]["emails"]:
            return
        profile = self._get_random_profile()
        email = random.choice(DATA_CACHE["velocity_test"]["emails"])
        token = str(uuid.uuid4())
        phone = f"{random.randint(10000000, 99999999)}"
        bin_value = random.choice(DATA_CACHE["velocity_test"]["bins"]) if DATA_CACHE["velocity_test"]["bins"] else "411111"
        payload = generate_payload(profile, email, phone, token, bin_value)
        self._send_request(payload, "Velocity-Email-CLAROPAY")

    @task(2)
    def test_phone_velocity(self):
        if not DATA_CACHE["velocity_test"]["phones"]:
            return
        profile = self._get_random_profile()
        phone = random.choice(DATA_CACHE["velocity_test"]["phones"])
        email = f"velocity_{random.randint(1000, 9999)}@example.com"
        token = str(uuid.uuid4())
        bin_value = random.choice(DATA_CACHE["velocity_test"]["bins"]) if DATA_CACHE["velocity_test"]["bins"] else "411111"
        payload = generate_payload(profile, email, phone, token, bin_value)
        self._send_request(payload, "Velocity-Phone-CLAROPAY")

    @task(1)
    def test_velocity_rules(self):
        profile = self._get_random_profile()
        email = f"rules_{random.randint(1000, 9999)}@example.com"
        phone = f"{random.randint(10000000, 99999999)}"
        token = str(uuid.uuid4())
        bin_value = random.choice(DATA_CACHE["velocity_test"]["bins"]) if DATA_CACHE["velocity_test"]["bins"] else "411111"
        payload = generate_payload(profile, email, phone, token, bin_value)
        self._send_request(payload, "Velocity-Rules-CLAROPAY")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logger.info("✅ Tests for ClaroPay in ClaroScore API: Initializing")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    logger.info("🛑 Tests for ClaroPay in ClaroScore API: Terminated")
    total = environment.stats.total.num_requests
    fail = environment.stats.total.fail_ratio
    logger.info(f"Total requests: {total}")
    logger.info(f"Failure rate: {fail:.2%}")