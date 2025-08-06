const axios = require("axios");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const XLSX = require("xlsx");
require("dotenv").config();

const apiToken = process.env.api_token;
const hiddenUrl = process.env.TransactionGET_BaseURL;

/**
 * Make API requests based on IDs from a CSV file, extract specified key-value pairs,
 * and save the results directly to Excel.
 *
 * @param {string} inputCsvPath - Path to the input CSV file containing IDs
 * @param {string} outputExcelPath - Path where the output Excel file will be saved
 * @param {string} urlTemplate - API endpoint URL template (ID will be appended)
 * @param {string} idColumn - Name of the column in the CSV containing the IDs (default: 'uuid')
 * @param {Object} headers - Headers for the API request including authorization
 * @param {Array} keysToExtract - List of keys to extract from the API response
 * @param {Object} columnMapping - Dictionary mapping original key paths to desired column names
 */
async function apiRequestWithExtraction(
  inputCsvPath,
  outputExcelPath,
  urlTemplate,
  idColumn = "uuid",
  headers = null,
  keysToExtract = null,
  columnMapping = null
) {
  if (!headers) {
    headers = {
      Authorization: "Bearer ",
      "Content-Type": "application/json",
    };
  }

  if (!keysToExtract) {
    keysToExtract = [];
  }

  if (!columnMapping) {
    columnMapping = {};
  }

  // Read IDs from the CSV file
  let dfInput;
  try {
    dfInput = await readCsv(inputCsvPath);
    if (!dfInput.some((row) => row.hasOwnProperty(idColumn))) {
      throw new Error(`Column '${idColumn}' not found in the input CSV file`);
    }
  } catch (error) {
    console.log(`Error reading input CSV: ${error.message}`);
    return;
  }

  // Request configuration
  const REQUEST_DELAY = 50; // Delay between requests in milliseconds

  // List to store all extracted data
  const extractedData = [];

  // Store the raw response data for debugging or additional processing
  const rawResponses = [];

  // Process each ID
  const totalIds = dfInput.length;
  for (let index = 0; index < dfInput.length; index++) {
    const row = dfInput[index];
    const idValue = String(row[idColumn]);

    // Format URL with current ID
    const url = `${urlTemplate}${idValue}`;

    console.log(`[${index + 1}/${totalIds}] Requesting URL: ${url}`);

    try {
      // Perform GET request
      const response = await axios.get(url, { headers });
      // await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY));

      if (response.status === 200) {
        const data = response.data;
        rawResponses.push(data);

        // Extract selected key-value pairs or all keys if none specified
        const extractedItem = {};
        extractedItem[idColumn] = idValue; // Always include the ID

        // Function to recursively search for keys in nested objects
        function extractNestedKeys(jsonObj, keyPath = "") {
          if (
            typeof jsonObj === "object" &&
            jsonObj !== null &&
            !Array.isArray(jsonObj)
          ) {
            for (const [k, v] of Object.entries(jsonObj)) {
              const currentPath = keyPath ? `${keyPath}.${k}` : k;

              // If this is a key we want, or we want all keys (empty keysToExtract)
              if (
                keysToExtract.length === 0 ||
                keysToExtract.includes(k) ||
                keysToExtract.includes(currentPath)
              ) {
                extractedItem[currentPath] = v;
              }

              // Continue recursion
              extractNestedKeys(v, currentPath);
            }
          } else if (Array.isArray(jsonObj)) {
            for (let i = 0; i < jsonObj.length; i++) {
              const currentPath = `${keyPath}[${i}]`;
              extractNestedKeys(jsonObj[i], currentPath);
            }
          }
        }

        // Extract keys
        extractNestedKeys(data);
        extractedData.push(extractedItem);
      } else {
        console.log(`Error for ID ${idValue}: Status code ${response.status}`);
        extractedData.push({
          [idColumn]: idValue,
          error: `Status code ${response.status}`,
        });
      }
    } catch (error) {
      console.log(`Exception for ID ${idValue}: ${error.message}`);
      extractedData.push({
        [idColumn]: idValue,
        error: error.message,
      });
    }
  }

  // Convert to Excel and save
  if (extractedData.length > 0) {
    try {
      // Apply column mapping if provided
      const mappedData = extractedData.map((row) => {
        const mappedRow = {};
        for (const [key, value] of Object.entries(row)) {
          const newKey = columnMapping[key] || key;
          mappedRow[newKey] = value;
        }
        return mappedRow;
      });

      // Create workbook and worksheet
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(mappedData);
      XLSX.utils.book_append_sheet(wb, ws, "Extracted Data");

      // Save to Excel
      XLSX.writeFile(wb, outputExcelPath);
      console.log(
        `Data successfully extracted and saved to ${outputExcelPath}`
      );

      // Also save raw responses for debugging or further processing
      const rawOutputPath = path.join(
        path.dirname(outputExcelPath),
        path.basename(outputExcelPath, path.extname(outputExcelPath)) +
          "_raw.xlsx"
      );

      const rawWb = XLSX.utils.book_new();
      const rawWs = XLSX.utils.json_to_sheet(
        rawResponses.map((resp) => ({ raw_response: JSON.stringify(resp) }))
      );
      XLSX.utils.book_append_sheet(rawWb, rawWs, "Raw Responses");
      XLSX.writeFile(rawWb, rawOutputPath);
      console.log(`Raw responses saved to ${rawOutputPath} for reference`);
    } catch (error) {
      console.log(`Error saving data to Excel: ${error.message}`);
    }
  } else {
    console.log("No data was extracted from the API responses");
  }
}

/**
 * Helper function to read CSV file and return array of objects
 * @param {string} filePath - Path to CSV file
 * @returns {Promise<Array>} Array of objects representing CSV rows
 */
function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", (error) => reject(error));
  });
}

// Main execution
async function main() {
  // Configuration - update these values as needed
  const inputCsvFile = path.join(
    require("os").homedir(),
    "Downloads",
    "Query-RyP-20250806.csv"
  );
  const timestamp = new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\..+/, "")
    .replace("T", "-");
    .replace("T", "-");
  const outputExcelFile = path.join(
    require("os").homedir(),
    "Downloads",
    `ExtractRes-RyP-${timestamp}.xlsx`
  );

  // API endpoint
  const urlTemplate = hiddenUrl;

  // Headers with authorization token
  const headers = {
    Authorization: `Bearer ${apiToken}`,
    "Content-Type": "application/json",
  };

  // Keys to extract
  const keysToExtract = [
    "data.transaccion.uuid",
    "data.transaccion.estatus",
    "data.transaccion.datos_claropagos.creacion",
    "data.transaccion.datos_procesador.capturas[0].respuesta.data.datetime",
    "data.transaccion.datos_comercio.pedido.id_externo",
    "data.transaccion.forma_pago",
    "data.transaccion.datos_pago.nombre",
    "data.transaccion.datos_pago.pan",
    "data.transaccion.datos_pago.marca",
    "data.transaccion.monto",
    "data.transaccion.moneda",
    "data.transaccion.pais",
    "data.transaccion.datos_procesador.data.all.data.orderId",
    "data.transaccion.datos_pago.plan_pagos.plan",
    "data.transaccion.datos_pago.plan_pagos.diferido",
    "data.transaccion.datos_pago.plan_pagos.parcialidades",
    "data.transaccion.datos_pago.plan_pagos.puntos",
    "data.transaccion.origen",
    "data.transaccion.operacion",
    "data.transaccion.datos_antifraude.resultado",
    "data.transaccion.datos_antifraude.procesador",
    "data.transaccion.datos_procesador.data.all.data.numero_autorizacion",
    "data.transaccion.datos_procesador.data.all.codigo",
    "data.transaccion.datos_procesador.data.all.tipo_transaccion",
    "data.transaccion.datos_procesador.data.codigo",
    "data.transaccion.datos_procesador.data.mensaje",
    "data.transaccion.datos_antifraude.datos_procesador[0].descripcion",
    "data.transaccion.afiliacion_uuid",
    "data.transaccion.datos_procesador.numero_afiliacion",
    "data.transaccion.datos_procesador.procesador",
    "data.transaccion.comercio_uuid",
    "data.transaccion.datos_claropagos.origin",
    "data.transaccion.datos_comercio.cliente.uuid",
    "data.transaccion.datos_comercio.cliente.direccion.telefono.numero",
    "data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto",
    "data.transaccion.conciliado",
    "data.transaccion.fecha_conciliacion",
  ];

  // Define column mapping to simplify header names
  const columnMapping = {
    "data.transaccion.uuid": "Id Transaccion",
    "data.transaccion.estatus": "Estatus",
    "data.transaccion.datos_claropagos.creacion": "Fecha y Hora",
    "data.transaccion.datos_procesador.capturas[0].respuesta.data.datetime":
      "Fecha Captura",
    "data.transaccion.datos_comercio.pedido.id_externo": "Id Externo/Pedido",
    "data.transaccion.forma_pago": "Forma de Pago",
    "data.transaccion.datos_pago.nombre": "Nombre Tarjethabiente",
    "data.transaccion.datos_pago.pan": "Pan",
    "data.transaccion.datos_pago.marca": "Marca Tarjeta",
    "data.transaccion.monto": "Monto",
    "data.transaccion.moneda": "Moneda",
    "data.transaccion.pais": "Pais",
    "data.transaccion.datos_procesador.data.all.data.orderId": "Orden",
    "data.transaccion.datos_pago.plan_pagos.plan": "Tipo de plan de pagos",
    "data.transaccion.datos_pago.plan_pagos.diferido": "Diferimiento",
    "data.transaccion.datos_pago.plan_pagos.parcialidades": "Mensualidades",
    "data.transaccion.datos_pago.plan_pagos.puntos": "Puntos",
    "data.transaccion.origen": "Origen de Transaccion",
    "data.transaccion.operacion": "Esquema",
    "data.transaccion.datos_antifraude.resultado": "Resultado Antifraude",
    "data.transaccion.datos_antifraude.procesador": "Procesador Antifraude",
    "data.transaccion.datos_procesador.data.all.data.numero_autorizacion":
      "Codigo Autorizacion",
    "data.transaccion.datos_procesador.data.all.codigo":
      "Codigo de Respuesta Procesador",
    "data.transaccion.datos_procesador.data.all.tipo_transaccion":
      "Tipo de Operacion",
    "data.transaccion.datos_procesador.data.codigo":
      "Codigo de Respuesta Claropagos",
    "data.transaccion.datos_procesador.data.mensaje":
      "Mensaje de Respuesta Claropagos",
    "data.transaccion.datos_antifraude.datos_procesador[0].descripcion":
      "Mensaje de Respuesta Antifraude",
    "data.transaccion.afiliacion_uuid": "Id Afiliacion",
    "data.transaccion.datos_procesador.numero_afiliacion": "Num Afiliacion",
    "data.transaccion.datos_procesador.procesador": "Procesador",
    "data.transaccion.comercio_uuid": "Id Comercio",
    "data.transaccion.datos_claropagos.origin": "Nombre Comercio",
    "data.transaccion.datos_comercio.cliente.uuid": "Id Cliente",
    "data.transaccion.datos_comercio.cliente.direccion.telefono.numero":
      "Num Telf",
    "data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto":
      "Id Producto",
    "data.transaccion.conciliado": "Cargo Conciliado",
    "data.transaccion.fecha_conciliacion": "Fecha Conciliacion",
  };

  // Run the function
  await apiRequestWithExtraction(
    inputCsvFile,
    outputExcelFile,
    urlTemplate,
    "uuid",
    headers,
    keysToExtract,
    columnMapping
  );
}

// Execute main function if this file is run directly
if (require.main === module) {
  main().catch(console.error);
}
