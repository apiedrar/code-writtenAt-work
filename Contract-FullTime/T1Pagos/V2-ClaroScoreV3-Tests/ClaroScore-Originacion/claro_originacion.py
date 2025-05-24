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
key_originacion = os.getenv("Originacion_ClaroScore")
hidden_url = os.getenv("originacion_api_url")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
EMAILS_XLSX_PATH = os.path.expanduser("~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/ClaroScore-Originacion/mailsclaro.xlsx")
OUTPUT_CSV = os.path.expanduser(f"~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/ClaroScore-Originacion/RespuestasTest_{timestamp}.csv")

headers = {
    "Content-Type": "application/json",
    "x-api-key": key_originacion
}

def generate_payload(email, phone_number):
    rand_int = lambda: random.randint(1, 10)
    rand_float = lambda: round(random.uniform(10.0, 999.99), 2)
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
                "number": phone_number
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
            "name": "Clara Luz",
            "paternal_surname": "Aguilar",
            "maternal_surname": "Hernandez",
            "email": email,
            "rfc": "VECJ880326MC",
            "gender": "Hombre",
            "birthdate": "1999-10-23",
            "phone": {  
                "number": phone_number
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
            "config": { k: True for k in [
              "bill_address_to_full_name_confidence", "bill_address_to_last_name_confidence",
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
        },
        "merchant": {
            "custom_1": "123e4567-e89b-4cab-a456-426614174000",
            "custom_2": "ABCD123456EFGH12",
            "custom_3": phone_number,
            "custom_4": "2001:db8::1",
            "custom_6": "550e8400-e29b-41d4-a716-446655440000",
            "custom_15": phone_number,
            "custom_21": "alonsojl@gmail.com",
            "custom_25": "12345-6789",
            "custom_31": "http://www.ejemplo.com"
        },
        "payment_method": {
            "type": "debit card",
            "card_token": "f4a233a6-9806-4654-9ad9-c0b8c8b94716",
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
                "number": phone_number
            }
        }
    }

def main():
    df = pd.read_excel(EMAILS_XLSX_PATH)
    emails = df.iloc[:, 0].dropna().tolist()

    phone_numbers = [
        "1111111111", "2222222222", "3333333333", "4444444444",
        "5555555555", "6666666666", "7777777777", "8888888888", "9999999999"
    ]

    results = []

    for email in emails:
        phone_number = random.choice(phone_numbers)
        payload = generate_payload(email, phone_number)
        try:
            response = requests.post(hidden_url, headers=headers, json=payload)
            print(f"Email: {email}, Phone: {phone_number}, Status: {response.status_code}")
            results.append({
                "email": email,
                "phone_number": phone_number,
                "status_code": response.status_code,
                "response": response.text
            })
        except Exception as e:
            results.append({
                "email": email,
                "phone_number": phone_number,
                "status_code": "ERROR",
                "response": str(e)
            })

    pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Test results saved in: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()