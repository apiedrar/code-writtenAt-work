import os
import csv
import pandas as pd
import unicodedata
import re

# DEFINE FILE PATHS HERE
INPUT_FILE = os.path.expanduser("~/Documents/MELI/Dia_31/ToProcess_10_31_2024.csv")  # CHANGE THIS to your input file path
OUTPUT_FILE = os.path.expanduser("~/Documents/MELI/Dia_31/Processed_10_31_2024.xlsx")  # CHANGE THIS to the path where you want to save the result

def has_accent(text):
    """Checks if a text has accents."""
    if not isinstance(text, str):
        return False
        
    # Pattern to detect accented characters in Spanish
    pattern = re.compile(r'[áéíóúüÁÉÍÓÚÜñÑ]')
    return bool(pattern.search(text))

def remove_accents(text):
    """Removes accents from text and reports if accents were found."""
    if not isinstance(text, str):
        return text
        
    # Check if it has accents before processing
    if has_accent(text):
        print(f"Accent detected in: '{text}'")
        
    # Normalize text to Unicode and remove diacritical characters
    text_without_accents = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    
    # If there's a difference, show before and after
    if text != text_without_accents:
        print(f"  → Changed to: '{text_without_accents}'")
        
    return text_without_accents

def process_excel():
    """Processes the Excel or CSV file, removes accents and saves to a new Excel file."""
    try:
        # Read the file according to its extension
        print(f"Reading file: {INPUT_FILE}")
        if INPUT_FILE.lower().endswith('.csv'):
            # For CSV files, explicitly specify the engine
            df = pd.read_csv(INPUT_FILE, engine='python')
        else:
            # For Excel files
            df = pd.read_excel(INPUT_FILE)
            
        # Count total cells with accents
        accent_counter = 0
        print("\nStarting accent detection...")
            
        # Apply the remove_accents function to all columns
        for column in df.columns:
            # Check if the column name has accents
            if has_accent(column):
                print(f"Column with accent: '{column}'")
                accent_counter += 1
                
            # Process each value in the column
            for i, value in enumerate(df[column]):
                if has_accent(str(value)):
                    accent_counter += 1
                df.at[i, column] = remove_accents(value)
                
        # Save the result to a new Excel file
        print(f"\nTotal cells with accents found: {accent_counter}")
        print(f"Saving result to: {OUTPUT_FILE}")
        df.to_excel(OUTPUT_FILE, index=False)
            
        print("Process completed successfully!")
        return True
    
    except Exception as e:
        print(f"Error processing the file: {str(e)}")
        return False

if __name__ == "__main__":
    # Process the file
    if process_excel():
        print(f"File without accents saved as: {OUTPUT_FILE}")
    else:
        print("The file could not be processed correctly.")