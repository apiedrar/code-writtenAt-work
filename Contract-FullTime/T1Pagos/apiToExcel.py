import requests
import csv
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
api_token = os.getenv('api_token')
hidden_url = os.getenv('APITransaction_URL')
def api_request_with_extraction(
    input_csv_path,
    output_excel_path,
    url_template,
    id_column='uuid',
    headers=None,
    keys_to_extract=None,
    column_mapping=None # Added the missing parameter
):
    """
    Make API requests based on IDs from a CSV file, extract specified key-value pairs,
    and save the results directly to Excel.
    
    Parameters:
    -----------
    input_csv_path : str
        Path to the input CSV file containing IDs
    output_excel_path : str
        Path where the output Excel file will be saved
    url_template : str
        API endpoint URL template (ID will be appended)
    id_column : str, optional
        Name of the column in the CSV containing the IDs (default: 'uuid')
    headers : dict, optional
        Headers for the API request including authorization
    keys_to_extract : list, optional
        List of keys to extract from the API response. If None, all keys will be included.
        column_mapping : dict, optional
        Dictionary mapping original key paths to desired column names
    """
    if headers is None:
        headers = {
            'Authorization': 'Bearer ',
            'Content-Type': 'application/json'
        }
    
    if keys_to_extract is None:
        keys_to_extract = []
            
    if column_mapping is None:
        column_mapping = {}
    
    # Read IDs from the CSV file
    try:
        df_input = pd.read_csv(input_csv_path)
        if id_column not in df_input.columns:
            raise ValueError(f"Column '{id_column}' not found in the input CSV file")
    except Exception as e:
        print(f"Error reading input CSV: {str(e)}")
        return

    # List to store all extracted data
    extracted_data = []
    
    # Store the raw response data for debugging or additional processing
    raw_responses = []
    
    # Process each ID
    total_ids = len(df_input)
    for index, row in df_input.iterrows():
        id_value = str(row[id_column])
        
        # Format URL with current ID
        url = f"{url_template}{id_value}"
        
        print(f"[{index+1}/{total_ids}] Requesting URL: {url}")
        
        try:
            # Perform GET request
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()  # Convert response to JSON
                raw_responses.append(data)  # Store raw response
                
                # Extract selected key-value pairs or all keys if none specified
                extracted_item = {}
                extracted_item[id_column] = id_value  # Always include the ID
                
                # Function to recursively search for keys in nested dictionaries
                def extract_nested_keys(json_obj, key_path=''):
                    if isinstance(json_obj, dict):
                        for k, v in json_obj.items():
                            current_path = f"{key_path}.{k}" if key_path else k
                            
                            # If this is a key we want, or we want all keys (empty keys_to_extract)
                            if not keys_to_extract or k in keys_to_extract or current_path in keys_to_extract:
                                extracted_item[current_path] = v
                            
                            # Continue recursion
                            extract_nested_keys(v, current_path)
                    elif isinstance(json_obj, list):
                        for i, item in enumerate(json_obj):
                            current_path = f"{key_path}[{i}]"
                            extract_nested_keys(item, current_path)
                
                # Extract keys
                extract_nested_keys(data)
                extracted_data.append(extracted_item)
            else:
                print(f'Error for ID {id_value}: Status code {response.status_code}')
                # Add a row with error information
                extracted_data.append({
                    id_column: id_value,
                    'error': f'Status code {response.status_code}'
                })
        except Exception as e:
            print(f'Exception for ID {id_value}: {str(e)}')
            extracted_data.append({
                id_column: id_value,
                'error': str(e)
            })
    
    # Convert to DataFrame and save to Excel
    if extracted_data:
        try:
            # Create DataFrame from extracted data
            df_output = pd.DataFrame(extracted_data)
            
            # Apply column mapping if provided
            if column_mapping:
                df_output = df_output.rename(columns=column_mapping)
            
            # Save to Excel
            df_output.to_excel(output_excel_path, index=False)
            print(f"Data successfully extracted and saved to {output_excel_path}")
            
            # Also save raw responses for debugging or further processing
            raw_output_path = os.path.splitext(output_excel_path)[0] + "_raw.xlsx"
            pd.DataFrame({'raw_response': [str(resp) for resp in raw_responses]}).to_excel(raw_output_path, index=False)
            print(f"Raw responses saved to {raw_output_path} for reference")
            
        except Exception as e:
            print(f"Error saving data to Excel: {str(e)}")
    else:
        print("No data was extracted from the API responses")

if __name__ == "__main__":
    # Configuration - update these values as needed
    input_csv_file = os.path.expanduser('~/Downloads/Query_RyPTelcel_20250521.csv')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%m")
    output_excel_file = os.path.expanduser(f'~/Downloads/ExtRes_RyPTelcel_20250521_{timestamp}.xlsx')
    
    # API endpoint - choose the one you need
    url_template = hidden_url
    
    # Headers with authorization token
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }
    
    # Keys to extract - add or remove keys as needed
    # For nested keys, use dot notation, e.g., 'customer.id_externo'
    keys_to_extract = [
        'data.transaccion.uuid',
        'data.transaccion.estatus',
        'data.transaccion.datos_claropagos.creacion',
        'data.transaccion.datos_procesador.capturas[0].respuesta.data.datetime',
        'data.transaccion.datos_comercio.pedido.id_externo',
        'data.transaccion.forma_pago',
        'data.transaccion.datos_pago.nombre',
        'data.transaccion.datos_pago.pan',
        'data.transaccion.datos_pago.marca',
        'data.transaccion.monto',
        'data.transaccion.moneda',
        'data.transaccion.pais',
        'data.transaccion.datos_procesador.data.all.data.orderId',
        'data.transaccion.datos_pago.plan_pagos.plan',
        'data.transaccion.datos_pago.plan_pagos.diferido',
        'data.transaccion.datos_pago.plan_pagos.parcialidades',
        'data.transaccion.datos_pago.plan_pagos.puntos',
        'data.transaccion.origen',
        'data.transaccion.operacion',
        'data.transaccion.datos_antifraude.resultado',
        'data.transaccion.datos_antifraude.procesador',
        'data.transaccion.datos_procesador.data.all.data.numero_autorizacion',
        'data.transaccion.datos_procesador.data.all.codigo',
        'data.transaccion.datos_procesador.data.all.tipo_transaccion',
        'data.transaccion.datos_procesador.data.codigo',
        'data.transaccion.datos_procesador.data.mensaje',
        'data.transaccion.datos_antifraude.descripcion',
        'data.transaccion.afiliacion_uuid',
        'data.transaccion.datos_procesador.numero_afiliacion',
        'data.transaccion.datos_procesador.procesador',
        'data.transaccion.comercio_uuid',
        'data.transaccion.datos_claropagos.origin',
        'data.transaccion.datos_comercio.cliente.uuid',
        'data.transaccion.datos_comercio.cliente.direccion.telefono.numero',
        'data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto',
        'data.transaccion.datos_comercio.pedido.direccion_envio.telefono.numero',
        'data.transaccion.conciliado',
        'data.transaccion.fecha_conciliacion'
        # Add any other keys you need
    ]
    
    # Define column mapping to simplify header names
    # Format: 'original_key_path': 'desired_column_name'
    column_mapping = {
        'data.transaccion.uuid': 'Id Transaccion',
        'data.transaccion.estatus': 'Estatus',
        'data.transaccion.datos_claropagos.creacion': 'Fecha y Hora',
        'data.transaccion.datos_procesador.capturas[0].respuesta.data.datetime': 'Fecha Captura',
        'data.transaccion.datos_comercio.pedido.id_externo': 'Id Externo/Pedido',
        'data.transaccion.forma_pago': 'Forma de Pago',
        'data.transaccion.datos_pago.nombre': 'Nombre Tarjethabiente',
        'data.transaccion.datos_pago.pan': 'Pan',
        'data.transaccion.datos_pago.marca': 'Marca Tarjeta',
        'data.transaccion.monto': 'Monto',
        'data.transaccion.moneda': 'Moneda',
        'data.transaccion.pais': 'Pais',
        'data.transaccion.datos_procesador.data.all.data.orderId': 'Orden',
        'data.transaccion.datos_pago.plan_pagos.plan': 'Tipo de plan de pagos',
        'data.transaccion.datos_pago.plan_pagos.diferido': 'Diferimiento',
        'data.transaccion.datos_pago.plan_pagos.parcialidades': 'Mensualidades',
        'data.transaccion.datos_pago.plan_pagos.puntos': 'Puntos',
        'data.transaccion.origen': 'Origen de Transaccion',
        'data.transaccion.operacion': 'Esquema',
        'data.transaccion.datos_antifraude.resultado': 'Resultado Antifraude',
        'data.transaccion.datos_antifraude.procesador': 'Procesador Antifraude',
        'data.transaccion.datos_procesador.data.all.data.numero_autorizacion': 'Codigo Autorizacion',
        'data.transaccion.datos_procesador.data.all.codigo': 'Codigo de Respuesta Procesador',
        'data.transaccion.datos_procesador.data.all.tipo_transaccion': 'Tipo de Operacion',
        'data.transaccion.datos_procesador.data.codigo': 'Codigo de Respuesta Claropagos',
        'data.transaccion.datos_procesador.data.mensaje': 'Mensaje de Respuesta Claropagos',
        'data.transaccion.datos_antifraude.descripcion': 'Mensaje de Respuesta Antifraude',
        'data.transaccion.afiliacion_uuid': 'Id Afiliacion',
        'data.transaccion.datos_procesador.numero_afiliacion': 'Num Afiliacion',
        'data.transaccion.datos_procesador.procesador': 'Procesador',
        'data.transaccion.comercio_uuid': 'Id Comercio',
        'data.transaccion.datos_claropagos.origin': 'Nombre Comercio',
        'data.transaccion.datos_comercio.cliente.uuid': 'Id Cliente',
        'data.transaccion.datos_comercio.cliente.direccion.telefono.numero': 'Num Telf',
        'data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto': 'Id Producto',
        'data.transaccion.datos_comercio.pedido.direccion_envio.telefono.numero': 'Telefono',
        'data.transaccion.conciliado': 'Cargo Conciliado',
        'data.transaccion.fecha_conciliacion': 'Fecha Conciliacion'
        # Add mappings for other keys as needed
    }
    
    # Run the combined function
    api_request_with_extraction(
        input_csv_file,
        output_excel_file,
        url_template,
        id_column='uuid',
        headers=headers,
        keys_to_extract=keys_to_extract,
        column_mapping=column_mapping
    )