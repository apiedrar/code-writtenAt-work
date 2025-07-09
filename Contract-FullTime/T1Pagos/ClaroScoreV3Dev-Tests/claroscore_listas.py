import os
import csv
import uuid
import random
from faker import Faker

# Initial configuration
fake = Faker()

# Base path where all files will be saved
BASE_PATH = os.path.expanduser("~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/Listas-CENAM")

# Directories to store test data
WHITE_LISTS_DIR = os.path.join(BASE_PATH, "listas_blancas")
BLACK_LISTS_DIR = os.path.join(BASE_PATH, "listas_negras")
REVIEW_LISTS_DIR = os.path.join(BASE_PATH, "listas_revision")

# Create directories if they don't exist
os.makedirs(WHITE_LISTS_DIR, exist_ok=True)  
os.makedirs(BLACK_LISTS_DIR, exist_ok=True)
os.makedirs(REVIEW_LISTS_DIR, exist_ok=True)

# Constants and configuration
MERCHANTS = {
    "CENAM": ["CENAM"]
}

class ListGenerator:
    """Class to generate the necessary lists for Claro Score"""
    
    def __init__(self):
        self.faker = Faker()
        # Common domains to generate more varied emails
        self.domains = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", 
                       "icloud.com", "protonmail.com", "aol.com", "mail.com"]
        
    def generate_all_lists(self):
        """Generates all necessary lists for each merchant"""
        print(f"Starting list generation for Claro Score at: {BASE_PATH}")
        
        # Generate lists by merchant and type
        for merchant in MERCHANTS:
            print(f"\nGenerating lists for merchant: {merchant}")
            
            # Generate email addresses
            self._generate_list_by_type(merchant, "emails", self._generate_email)
            
            # Generate phone numbers
            self._generate_list_by_type(merchant, "phones", self._generate_phone)
            
            # Generate card tokens
            self._generate_list_by_type(merchant, "tokens", self._generate_token)
        
        print("\nAll lists have been generated successfully!")
    
    def _generate_list_by_type(self, merchant, data_type, generator_function):
        """Generates white, black, and review lists for a specific data type"""
        list_types = {
            WHITE_LISTS_DIR: "white",
            BLACK_LISTS_DIR: "black",
            REVIEW_LISTS_DIR: "review"
        }
        
        for directory, list_name in list_types.items():
            filename = os.path.join(directory, f"{merchant}_{data_type}.csv")
            print(f"  Generating {list_name} list of {data_type}...")
            
            with open(filename, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                # Header according to data type
                if data_type == "emails":
                    writer.writerow(["correo_electronico"])
                elif data_type == "phones":
                    writer.writerow(["telefono"])
                else:  # tokens
                    writer.writerow(["token_tarjeta"])
                
                # Generate 100,000 unique records
                unique_items = set()
                
                # Use a counter to show progress
                print("    Generating 100 records...")
                for i in range(100):
                    if i > 0 and i % 10 == 0:
                        print(f"    {i} records generated...")
                    
                    # Generate a new unique item
                    while True:
                        new_item = generator_function()
                        if new_item not in unique_items:
                            unique_items.add(new_item)
                            writer.writerow([new_item])
                            break
            
            print(f"  ✓ List saved at: {filename}")
    
    def _generate_email(self):
        """Generates a random email address"""
        # Use different strategies for greater variety
        if random.random() < 0.7:
            # Standard Faker email
            return self.faker.email()
        else:
            # Custom email with common patterns
            first_name = self.faker.first_name().lower()
            last_name = self.faker.last_name().lower()
            domain = random.choice(self.domains)
            
            format_type = random.choice([
                f"{first_name}.{last_name}@{domain}",
                f"{first_name}{last_name}@{domain}",
                f"{first_name}_{last_name}@{domain}",
                f"{first_name[0]}{last_name}@{domain}",
                f"{first_name}{random.randint(1, 999)}@{domain}"
            ])
            
            return format_type
    
    def _generate_phone(self):
        """Generates a valid Mexican phone number"""
        # Format for Mexico: AREA_CODE (2 or 3 digits) + number (8 or 7 digits)
        format_type = random.choice([
            "{}{}",     # Only area code and number
        ])
        
        # Generate area code
        common_area_codes = ["55", "56", "33", "81", "222", "442", "664", "998", "999", "477"]
        if random.random() < 0.7:
            # Use a common area code
            area_code = random.choice(common_area_codes)
        else:
            # Generate a random 2 or 3 digit area code
            area_code_length = random.choice([2, 3])
            if area_code_length == 2:
                area_code = str(random.randint(10, 99))
            else:
                area_code = str(random.randint(100, 999))
        
        # Generate number (8 digits for 2-digit area code, 7 digits for 3-digit area code)
        if len(area_code) == 2:
            number = str(random.randint(10000000, 99999999))
        else:
            number = str(random.randint(1000000, 9999999))
        
        return format_type.format(area_code, number)
    
    def _generate_token(self):
        """Generates a card token (UUID)"""
        return str(uuid.uuid4())

if __name__ == "__main__":
    # Verify that the base path exists
    if not os.path.exists(BASE_PATH):
        print(f"WARNING! The base path does not exist: {BASE_PATH}")
        print("Attempting to create the base path...")
        try:
            os.makedirs(BASE_PATH, exist_ok=True)
            print(f"Base path created successfully: {BASE_PATH}")
        except Exception as e:
            print(f"Error creating base path: {e}")
            print("Please manually create the directory or check permissions.")
            exit(1)
    
    # Start list generation
    generator = ListGenerator()
    generator.generate_all_lists()
    
    # Show summary of generated files
    print("\nSummary of generated files:")
    for folder_name, folder_path in [
        ("WHITE LISTS", WHITE_LISTS_DIR), 
        ("BLACK LISTS", BLACK_LISTS_DIR), 
        ("REVIEW LISTS", REVIEW_LISTS_DIR)
    ]:
        files = os.listdir(folder_path)
        print(f"\n{folder_name}:")
        for file in files:
            path = os.path.join(folder_path, file)
            size = os.path.getsize(path) / (1024 * 1024)  # Size in MB
            print(f"  - {file} ({size:.2f} MB)")