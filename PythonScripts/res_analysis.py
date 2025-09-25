import pandas as pd
import os
import sys
import argparse


def main():
    # ========================
    # ARGUMENTOS DE LÍNEA DE COMANDOS
    # ========================
    parser = argparse.ArgumentParser(
        description="Procesar archivos de respuestas y transacciones Telcel"
    )
    parser.add_argument("respuestas", help="Ruta al archivo de respuestas (.xlsx)")
    parser.add_argument(
        "transacciones", help="Ruta al archivo de transacciones (.xlsx)"
    )
    parser.add_argument("salida", help="Ruta del archivo de salida (.xlsx)")

    args = parser.parse_args()

    # ========================
    # ARCHIVOS
    # ========================
    file_respuestas = args.respuestas
    file_transacciones = args.transacciones
    file_salida = args.salida

    # Columnas
    col_tel_resp = "Telefono"
    col_resp = "Mensaje"
    col_fecha_resp = "Fecha"
    col_tel_trans = "Telefono"
    col_trans = "No. Externo/Pedido"

    # ========================
    # VERIFICAR QUE LOS ARCHIVOS EXISTEN
    # ========================
    if not os.path.exists(file_respuestas):
        print(f"❌ Error: No se encuentra el archivo de respuestas: {file_respuestas}")
        sys.exit(1)

    if not os.path.exists(file_transacciones):
        print(
            f"❌ Error: No se encuentra el archivo de transacciones: {file_transacciones}"
        )
        sys.exit(1)

    # ========================
    # LEER EXCELS
    # ========================
    try:
        df_resp = pd.read_excel(file_respuestas, dtype=str)
        df_trans = pd.read_excel(file_transacciones, dtype=str)
    except Exception as e:
        print(f"❌ Error al leer los archivos: {e}")
        sys.exit(1)

    # ========================
    # CONVERTIR FECHA ISO A SOLO DÍA
    # ========================
    df_resp[col_fecha_resp] = pd.to_datetime(
        df_resp[col_fecha_resp], errors="coerce"
    ).dt.date

    # ========================
    # FILTRAR SOLO TELEFONOS COMUNES
    # ========================
    telefonos_comunes = set(df_resp[col_tel_resp]).intersection(
        set(df_trans[col_tel_trans])
    )
    df_resp_filtrado = df_resp[df_resp[col_tel_resp].isin(telefonos_comunes)]
    df_trans_filtrado = df_trans[df_trans[col_tel_trans].isin(telefonos_comunes)]

    # ========================
    # OBTENER PROCESADOR POR TELEFONO
    # ========================
    df_procesador = df_trans_filtrado.drop_duplicates(subset=[col_tel_trans])
    df_procesador = df_procesador[[col_tel_trans, "No. Externo/Pedido"]]
    df_procesador["Pedido-Canal"] = (
        df_procesador["No. Externo/Pedido"].str.split("-").str[-1]
    )
    df_procesador = df_procesador[[col_tel_trans, "Pedido-Canal"]]

    # ========================
    # AGRUPAR RESPUESTAS POR DIA + TELEFONO + RESPUESTA
    # ========================
    df_resp_contada = (
        df_resp_filtrado.groupby([col_fecha_resp, col_tel_resp, col_resp])
        .size()
        .reset_index(name="count")
    )

    # ========================
    # ASOCIAR PROCESADOR
    # ========================
    df_final = pd.merge(
        df_resp_contada,
        df_procesador,
        left_on=col_tel_resp,
        right_on=col_tel_trans,
        how="left",
    )

    # ========================
    # AGRUPAR POR DIA + PROCESADOR + RESPUESTA
    # ========================
    df_resultado = (
        df_final.groupby([col_fecha_resp, "Pedido-Canal", col_resp])["count"]
        .sum()
        .reset_index(name="Total")
    )

    # ========================
    # EXPORTAR RESULTADO
    # ========================
    try:
        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(file_salida), exist_ok=True)
        df_resultado.to_excel(file_salida, index=False)
        print(f"✅ Proceso completado. Archivo generado: {file_salida}")
    except Exception as e:
        print(f"❌ Error al guardar el archivo: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
