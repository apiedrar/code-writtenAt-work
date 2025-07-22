import csv
import uuid
import random
import argparse
import string
from typing import List, Dict, Any


def generate_uuid() -> str:
    """Generate a random UUID."""
    return str(uuid.uuid4())


def generate_ean_upc() -> str:
    """Generate a random EAN/UPC code."""
    # Format like "4011 200296908"
    first_part = ''.join(random.choices(string.digits, k=4))
    second_part = ''.join(random.choices(string.digits, k=9))
    return f"{first_part} {second_part}"


def generate_row() -> Dict[str, Any]:
    """Generate a row of random data for the CSV file."""
    return {
        "commerce_id": random.randint(1, 10),
        "client_id": generate_uuid(),
        "ean_upc": generate_ean_upc(),
        "client.name": random.choice(["Carlos", "Maria", "Juan", "Ana", "Miguel", "Sofia"]),
        "client.paternal_surname": random.choice(["Rodriguez", "Gonzalez", "Lopez", "Martinez", "Garcia"]),
        "client.maternal_surname": random.choice(["Perez", "Sanchez", "Ramirez", "Torres", "Diaz"]),
        "client.email": f"{generate_uuid().split('-')[0]}@example.com",
        "purchase.items.0.name": random.choice(["Lentes", "Reloj", "Bolsa", "Zapatos", "Camisa", "Pantalon"]),
        "purchase.items.0.sku": ''.join(random.choices(string.digits, k=5)),
    }


def generate_csv(num_rows: int, output_file: str):
    """Generate a CSV file with the specified number of rows."""
    # Generate sample rows
    rows = [generate_row() for _ in range(num_rows)]
    
    # Get all keys from all rows (some rows might have different keys)
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    
    # Sort keys for consistency
    sorted_keys = sorted(all_keys)
    
    # Write rows to CSV
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Successfully generated CSV file with {num_rows} rows: {output_file}")
    except Exception as e:
        print(f"Error generating CSV file: {e}")


def generate_empty_template(output_file: str):
    """Generate an empty CSV template with just the headers."""
    # Define the standard fields that should be included in the template
    fields = [
        "commerce_id",
        "client_id",
        "ean_upc",
        "client.name",
        "client.paternal_surname",
        "client.maternal_surname",
        "client.email",
        "client.phone.country_code",
        "client.phone.area_code",
        "client.phone.number",
        "purchase.items.0.name",
        "purchase.items.0.sku",
        "purchase.items.0.quantity",
        "purchase.items.0.unit_amount"
    ]
    
    # Write empty template to CSV
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(fields)
        print(f"Successfully generated empty CSV template: {output_file}")
    except Exception as e:
        print(f"Error generating CSV template: {e}")


def main():
    parser = argparse.ArgumentParser(description="Generate CSV data for payload sender.")
    parser.add_argument("--output", default="payload_data.csv", help="Output CSV file")
    parser.add_argument("--rows", type=int, default=10, help="Number of rows to generate")
    parser.add_argument("--empty", action="store_true", help="Generate an empty template")
    
    args = parser.parse_args()
    
    if args.empty:
        generate_empty_template(args.output)
    else:
        generate_csv(args.rows, args.output)


if __name__ == "__main__":
    main()
