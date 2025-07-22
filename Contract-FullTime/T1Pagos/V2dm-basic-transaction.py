import json
import os
import time
import pandas as pd
import csv
import sys
import logging as python_logging  # Renombrado para evitar conflictos
from CyberSource import *
from pathlib import Path
from importlib.machinery import SourceFileLoader
import gc
import urllib3

# Configure connection pooling for urllib3 (used by requests/CyberSource)
urllib3.PoolManager(maxsize=50, block=True)

# Desactivar o configurar logging para evitar errores
python_logging.getLogger('CyberSource').setLevel(python_logging.ERROR)
python_logging.getLogger('urllib3').setLevel(python_logging.ERROR)
python_logging.getLogger('requests').setLevel(python_logging.ERROR)

# Crear directorio de logs si no existe
log_dir = os.path.join(os.getcwd(), "Logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Intentar crear archivos de log vacíos para evitar FileNotFoundError
for i in range(1, 11):  # Crear logs del 1 al 10
    log_file = os.path.join(log_dir, f"cybs.log.{i}")
    if not os.path.exists(log_file):
        try:
            with open(log_file, 'w') as f:
                pass  # Crear archivo vacío
        except:
            pass  # Ignorar errores al crear archivos

# Cargar configuración de CyberSource
config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

# Configuración global para reutilización
config_obj = configuration.Configuration()
client_config = config_obj.get_configuration()

def del_none(d):
    """Elimina los valores None de un diccionario recursivamente"""
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d

def process_transaction(row, api_instance):
    """Procesa una transacción a través de CyberSource"""
    try:
        # Convertir valores a string para asegurar compatibilidad
        for key in row:
            if pd.isna(row[key]):
                row[key] = ""
            else:
                row[key] = str(row[key])
        
        clientReferenceInformation = Riskv1decisionsClientReferenceInformation(
            code=row['id']
        )

        paymentInformationCard = Riskv1decisionsPaymentInformationCard(
            bin=row['bin'],
        )
        paymentInformation = Riskv1decisionsPaymentInformation(
            card=paymentInformationCard.__dict__
        )

        orderInformationAmountDetails = Riskv1decisionsOrderInformationAmountDetails(
            currency=row['currency__id'],
            total_amount=row['local_currency_amt']
        )
        
        orderInformationShipTo = Riskv1decisionsOrderInformationShipTo(
            address1=row['shipping_address'],
            administrative_area=row['shipping_state'],
            country=row['shipping_country'],
            locality=row['shipping_city'],
            phone_number=row['shipping_phone_number'],
            postal_code=row['shipping_zip_code']
        )
        
        orderInformationBillTo = Riskv1decisionsOrderInformationBillTo(
            address1=str(row['address_number']) + ' ' + str(row['address_street']),
            administrative_area=row['address_state'],
            country=row['address_country'],
            locality=row['address_city'],
            first_name=row['first_name'],
            last_name=row['last_name'],
            phone_number=row['phone_number'],
            email=row['email'],
            postal_code=row['address_zip_code']
        )

        orderInformationLineItems = Riskv1addressverificationsOrderInformationLineItems(
            quantity=row['items_quantity'],
            product_name=row['item_name']
        )

        orderInformation = Riskv1decisionsOrderInformation(
            amount_details=orderInformationAmountDetails.__dict__,
            ship_to=orderInformationShipTo.__dict__,
            bill_to=orderInformationBillTo.__dict__,
            line_items=orderInformationLineItems.__dict__,
        )
        
        requestObj = CreateBundledDecisionManagerCaseRequest(
            client_reference_information=clientReferenceInformation.__dict__,
            payment_information=paymentInformation.__dict__,
            order_information=orderInformation.__dict__
        )
        
        requestObj = del_none(requestObj.__dict__)
        requestObj = json.dumps(requestObj)
            
        # Bloque try-except específico para la llamada API
        try:
            return_data, status, body = api_instance.create_bundled_decision_manager_case(requestObj)
            return status, body
        except Exception as api_error:
            # Capturar errores específicos de la API
            print(f"Error en llamada API: {api_error}")
            return api_error.status if hasattr(api_error, 'status') else 999, str(api_error)
            
    except Exception as e:
        # Capturar cualquier otro error en la preparación
        import traceback
        print(f"Error preparando transacción: {e}")
        traceback.print_exc()
        return 999, str(e)

def process_in_batches(rows, output_csv, fieldnames, batch_size=50):
    """Procesa los registros en lotes para mejor manejo de recursos"""
    total_rows = len(rows)
    
    # Crear una sola instancia de API para reutilizar
    api_instance = DecisionManagerApi(client_config)
    
    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = rows[batch_start:batch_end]
        results = []
        
        # Procesar el lote
        for i, row in enumerate(batch):
            row_index = batch_start + i
            
            # Procesar la transacción
            status, body = process_transaction(row, api_instance)
            
            # Añadir la respuesta al registro
            result = row.copy()  # Crear una copia para no modificar el original
            result['response_status'] = status
            result['response_body'] = body
            results.append(result)
            
            # Mostrar progreso con el email incluido
            email = row.get('email', 'N/A')
            print(f"Transacción {row_index+1}/{total_rows}: Status {status}, Email: {email}")
            
            # Pequeña pausa para no sobrecargar
            time.sleep(0.1)
        
        # Escribir todo el lote junto en el CSV
        with open(output_csv, 'a', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(results)
        
        # Limpiar memoria después de cada lote
        del results
        gc.collect()
        
        print(f"Procesado lote {batch_start+1}-{batch_end} de {total_rows}")

def excel_to_csv_processor(input_excel, output_csv):
    """
    Procesa un archivo Excel, extrae los datos, hace llamadas a la API y guarda los resultados en CSV
    Este método evita mantener el Excel abierto durante todo el procesamiento
    """
    try:
        # Convertir Excel a lista de diccionarios (una sola operación de Excel)
        print(f"Leyendo archivo Excel: {input_excel}")
        df = pd.read_excel(input_excel)
        rows = df.to_dict('records')
        print(f"Archivo Excel convertido a {len(rows)} registros")
        
        # Cerrar cualquier conexión con el Excel
        del df
        gc.collect()
        
        # Preparar el archivo CSV de salida
        fieldnames = list(rows[0].keys()) + ['response_status', 'response_body']
        
        # Crear archivo CSV con encabezados
        with open(output_csv, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        
        # Procesar registros en lotes para mejor manejo de recursos
        process_in_batches(rows, output_csv, fieldnames, batch_size=50)
        
        print(f"Procesamiento completado. Resultados guardados en: {output_csv}")
        return True
        
    except Exception as e:
        import traceback
        print(f"Error en el procesamiento: {e}")
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Definir rutas de archivos
    input_excel = os.path.expanduser('~/Documents/MELI/Dia_31/Meli_Dia31_Pt1.xlsx')
    output_csv = os.path.expanduser('~/Documents/MELI/Dia_31/Respuestas/Meli_Dia31_Pt1_respuestas.csv')
    
    # Crear carpeta de logs si no existe
    logs_path = os.path.expanduser('~/Documents/Code-Scripts/Cybersource/cybersource-rest-samples-python-master/Logs')
    if not os.path.exists(logs_path):
        try:
            os.makedirs(logs_path)
        except:
            print(f"No se pudo crear el directorio de logs: {logs_path}")
    
    # Crear archivos de log vacíos para evitar el error
    for i in range(1, 11):
        log_file = os.path.join(logs_path, f"cybs.log.{i}")
        try:
            if not os.path.exists(log_file):
                with open(log_file, 'w') as f:
                    pass  # Crear archivo vacío
        except:
            print(f"No se pudo crear archivo de log: {log_file}")
    
    # Desactivar logs antes de iniciar
    for logger_name in ['CyberSource', 'urllib3', 'requests']:
        python_logging.getLogger(logger_name).setLevel(python_logging.CRITICAL)
    
    # Aumentar el límite de archivos abiertos para este proceso
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"Límite actual de archivos: {soft} (soft), {hard} (hard)")
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(8192, hard), hard))
        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"Nuevo límite de archivos: {new_soft} (soft), {new_hard} (hard)")
    except Exception as e:
        print(f"No se pudo cambiar el límite de archivos: {e}")
    
    # Procesar el archivo
    print("=== INICIANDO PROCESAMIENTO ===")
    excel_to_csv_processor(input_excel, output_csv)
    print("=== PROCESAMIENTO FINALIZADO ===")