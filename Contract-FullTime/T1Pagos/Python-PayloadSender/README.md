# Payload Sender

This package contains scripts for sending payloads based on a specified template, with support for custom data from CSV files.

## Requirements

Install the required packages:

```bash
pip install requests
```

## File Structure

- `payload_sender.py`: Main script for sending payloads
- `data_generator.py`: Script for generating sample data
- `payload_data.csv`: Sample CSV data template

## Usage Instructions

### 1. Generating Data

You can generate sample data or an empty template using the `data_generator.py` script:

```bash
# Generate 10 rows of sample data
python data_generator.py --output payload_data.csv --rows 10

# Generate an empty template
python data_generator.py --output template.csv --empty
```

### 2. Preparing Custom Data

Edit the generated CSV file (or create your own) with the required data. The CSV should include columns for:
- `commerce_id`: The commerce ID
- `client_id`: The client ID
- `ean_upc`: The EAN/UPC code
- Any other custom fields using dot notation (e.g., `client.name`, `purchase.items.0.quantity`)

### 3. Sending Payloads

Use the `payload_sender.py` script to send payloads:

```bash
# Send a single request with data from a CSV file
python payload_sender.py --url "https://api.example.com/endpoint" --api-key "your-api-key" --csv-file payload_data.csv

# Send multiple requests
python payload_sender.py --url "https://api.example.com/endpoint" --api-key "your-api-key" --num-requests 5 --csv-file payload_data.csv

# Add delay between requests (in seconds)
python payload_sender.py --url "https://api.example.com/endpoint" --api-key "your-api-key" --num-requests 10 --delay 2.5 --csv-file payload_data.csv

# Save responses to a file
python payload_sender.py --url "https://api.example.com/endpoint" --api-key "your-api-key" --num-requests 3 --output responses.json --csv-file payload_data.csv
```

## CSV Field Structure

The CSV file uses dot notation to specify nested fields in the payload. For example:

- `client.name` refers to `payload["client"]["name"]`
- `purchase.items.0.sku` refers to `payload["purchase"]["items"][0]["sku"]`

## Additional Notes

- Fields not specified in the CSV will use random values or defaults.
- You can add more fields to the CSV as needed, following the same dot notation pattern.
- For array items beyond the first one, use index notation (e.g., `purchase.items.1.name`).
