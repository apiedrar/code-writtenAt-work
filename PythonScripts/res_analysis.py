import pandas as pd
import os
import sys
import argparse


def main():
    # ========================
    # COMMAND LINE ARGUMENTS
    # ========================
    parser = argparse.ArgumentParser(
        description="Process Telcel's Response messages and Transactions files"
    )
    parser.add_argument("responses", help="Path to responses file (.xlsx)")
    parser.add_argument("transactions", help="Path to transactions file (.xlsx)")
    parser.add_argument("output", help="Path to output file (.xlsx)")

    args = parser.parse_args()

    # ========================
    # FILES
    # ========================
    responses_file = args.responses
    transactions_file = args.transactions
    output_file = args.output

    # Columns
    res_phone_col = "Telefono"
    res_col = "Mensaje"
    res_date_col = "Fecha"
    trx_phone_col = "Telefono"
    trx_col = "No. Externo/Pedido"

    # ========================
    # FILES IN PATH VALIDATION
    # ========================
    if not os.path.exists(responses_file):
        print(f"❌ Error: Responses file not found: {responses_file}")
        sys.exit(1)

    if not os.path.exists(transactions_file):
        print(f"❌ Error: Transactions file not found: {transactions_file}")
        sys.exit(1)

    # ========================
    # READ EXCELS
    # ========================
    try:
        df_res = pd.read_excel(responses_file, dtype=str)
        df_trx = pd.read_excel(transactions_file, dtype=str)
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        sys.exit(1)

    # ========================
    # CONVERT DATE TO DAY ONLY
    # ========================
    df_res[res_date_col] = pd.to_datetime(df_res[res_date_col], errors="coerce").dt.date

    # ========================
    # FILTER ONLY COMMON PHONE NUMBERS
    # ========================
    common_phone_numbers = set(df_res[res_phone_col]).intersection(
        set(df_trx[trx_phone_col])
    )
    df_res_filter = df_res[df_res[res_phone_col].isin(common_phone_numbers)]
    df_trx_filter = df_trx[df_trx[trx_phone_col].isin(common_phone_numbers)]

    # ========================
    # GET PROCESSOR BY PHONE NUMBER
    # ========================
    df_processor = df_trx_filter.drop_duplicates(subset=[trx_phone_col])
    df_processor = df_processor[[trx_phone_col, "No. Externo/Pedido"]]
    df_processor["Pedido-Canal"] = (
        df_processor["No. Externo/Pedido"].str.split("-").str[-1]
    )
    df_processor = df_processor[[trx_phone_col, "Pedido-Canal"]]

    # ========================
    # GROUP RESPONSES BY DAY + PHONE NUMBER + MESSAGE
    # ========================
    df_res_to_count = (
        df_res_filter.groupby([res_date_col, res_phone_col, res_col])
        .size()
        .reset_index(name="count")
    )

    # ========================
    # MAKE PROCESSOR MATCH
    # ========================
    df_final = pd.merge(
        df_res_to_count,
        df_processor,
        left_on=res_phone_col,
        right_on=trx_phone_col,
        how="left",
    )

    # ========================
    # GROUP BY DAY + PROCESSOR + MESSAGE
    # ========================
    df_result = (
        df_final.groupby([res_date_col, "Pedido-Canal", res_col])["count"]
        .sum()
        .reset_index(name="Total")
    )

    # ========================
    # EXPORT RESULT
    # ========================
    try:
        # Create output directory in case it's missing
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df_result.to_excel(output_file, index=False)
        print(f"✅ Process complete. Generated file: {output_file}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
