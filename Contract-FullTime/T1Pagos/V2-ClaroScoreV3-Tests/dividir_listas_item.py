import os
import pandas as pd

# Load CSV file
df = pd.read_csv(os.path.expanduser("~/Documents/Code-Scripts/Work/Contract-FullTime/T1Pagos/listas_revision/SEARS_tokens.csv"))

# Rename column header to 'Item'
df.columns = ['Item']

# Separate into 10,000 rows files
chunk_size = 10000
for i in range(0, len(df), chunk_size):
    df_chunk = df.iloc[i:i+chunk_size]
    df_chunk.to_csv(f"tokens_SEARS_negra_{i//chunk_size + 1}.csv", index=False, header=['Item'])