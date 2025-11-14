import pandas as pd
import json

# Cargar EXCEL
df = pd.read_excel('/Users/claropagos10/Downloads/10_31_2024_respues.xlsx')

# Extraer datos del JSON
extracted_data = []

for index, row in df.iterrows():
    try:
        json_data = json.loads(row['response_body'])
        
        # Crear un diccionario con los datos extraídos
        extracted = {
            'reference_code': json_data.get('clientReferenceInformation', {}).get('code'),
            'status': json_data.get('status'),
            'risk_score': json_data.get('riskInformation', {}).get('score', {}).get('result'),
            'early_decision': json_data.get('riskInformation', {}).get('profile', {}).get('earlyDecision'),
            'rejection_reason': json_data.get('errorInformation', {}).get('reason'),
            'payment_scheme': json_data.get('paymentInformation', {}).get('scheme'),
            'payment_bin': json_data.get('paymentInformation', {}).get('bin'),
            'emailage_score': json_data.get('riskInformation', {}).get('providers', {}).get('emailage', {}).get('ea_score'),
            'elephant_decision': json_data.get('riskInformation', {}).get('providers', {}).get('elephant', {}).get('decision')
            # Puedes agregar más campos según necesites
        }
        
        # Añadir los valores originales para mantener el contexto
        extracted.update(row.to_dict())
        extracted_data.append(extracted)
    except:
        # Si hay un error al parsear el JSON, mantener los datos originales
        extracted_data.append(row.to_dict())

# Crear un nuevo DataFrame con los datos extraídos
result_df = pd.DataFrame(extracted_data)

# Guardar a un nuevo archivo Excel
result_df.to_csv('/Users/claropagos10/Downloads/10_31_2024_respuestas1.csv', index=False)