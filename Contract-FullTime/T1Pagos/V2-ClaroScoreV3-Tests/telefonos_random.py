import os
import random
import csv

volume = 100_000

phone_numbers = set()

while len(phone_numbers) < volume:
    # First digit from 1 to 9
    first_digit = str(random.randint(5, 9))
    # Following 7 digits from 0 to 9
    remain = ''.join([str(random.randint(0, 9)) for _ in range(7)])
    number = str(56) + first_digit + remain
    phone_numbers.add(number)

# Save to CSV file
with open(os.path.expanduser('~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/V2-ClaroScoreV3-Tests/telefonos_RyP_Negativa.csv'), mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['phone number'])  # Header
    for phone_number in phone_numbers:
        writer.writerow([phone_numbers])

print(f'{volume} unique 8 digit phone numbers have been generated and saved to "telefonos_8_digitos_unicos.csv".')












