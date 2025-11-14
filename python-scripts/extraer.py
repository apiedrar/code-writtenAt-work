import pandas as pd
import os
import json

# Cargar EXCEL
df = pd.read_excel(
    os.path.expanduser("~/Downloads/POC-Inbursa/Res/Res-POC-Inbursa-20250923-ReDo.xlsx")
)

# Extraer datos del JSON
extracted_data = []

for index, row in df.iterrows():
    try:
        json_data = json.loads(row["response_body"])

        # Extraer únicamente los campos de emailage
        emailage_data = (
            json_data.get("riskInformation", {})
            .get("providers", {})
            .get("emailage", {})
        )

        extracted = {
            "mobile": row.get("asdasd"),
            "email": row.get("CORREO"),
            "reference_code": json_data.get("clientReferenceInformation", {}).get(
                "code"
            ),
            "status": json_data.get("status"),
            "ea_score": emailage_data.get("ea_score"),
            "ea_reason": emailage_data.get("ea_reason"),
            "ea_reason_id": emailage_data.get("ea_reason_id"),
            "ea_risk_band_id": emailage_data.get("ea_risk_band_id"),
            "ea_advice": emailage_data.get("ea_advice"),
            "country": emailage_data.get("country"),
            "domain_name": emailage_data.get("domain_name"),
            "domain_category": emailage_data.get("domain_category"),
            "domain_age": emailage_data.get("domain_age"),
            "domain_creation_days": emailage_data.get("domain_creation_days"),
            "domain_exists": emailage_data.get("domain_exists"),
            "domain_risk_level": emailage_data.get("domain_risk_level"),
            "domain_corporate": emailage_data.get("domain_corporate"),
            "email_exists": emailage_data.get("email_exists"),
            "first_verification_date": emailage_data.get("first_verification_date"),
            "first_seen_days": emailage_data.get("first_seen_days"),
        }

        extracted_data.append(extracted)
    except:
        # Si no se puede parsear, guardar fila vacía con referencia
        extracted_data.append(
            {"reference_code": row.get("reference_code", None), "status": None}
        )

# Crear DataFrame solo con los datos de emailage
result_df = pd.DataFrame(extracted_data)

# Guardar en CSV
result_df.to_csv(
    os.path.expanduser(
        "~/Downloads/POC-Inbursa/Res/Resultados-POC-Inbursa-20250923-ReDo.xlsx"
    ),
    index=False,
)

print("✅ Archivo generado con los datos de emailage")
