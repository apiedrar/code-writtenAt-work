const axios = require("axios");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const XLSX = require("xlsx");
require("dotenv").config();

const apiToken = process.env.api_token;
const hiddenUrl = process.env.TransactionGET_BaseURL;

/**
 * Make API requests based on IDs from a CSV or Excel file, extract specified key-value pairs,
 * and save the results directly to Excel.
 *
 * @param {string} inputFilePath - Path to the input CSV or Excel file containing IDs
 * @param {string} outputExcelPath - Path where the output Excel file will be saved
 * @param {string} urlTemplate - API endpoint URL template (ID will be appended)
 * @param {string} idColumn - Name of the column in the file containing the IDs (default: 'uuid')
 * @param {Object} headers - Headers for the API request including authorization
 * @param {Array} keysToExtract - List of keys to extract from the API response
 * @param {Object} columnMapping - Dictionary mapping original key paths to desired column names
 * @param {string} sheetName - Name of the Excel sheet to read (default: first sheet)
 */
async function apiRequestWithExtraction(
  inputFilePath,
  outputExcelPath,
  urlTemplate,
  idColumn = "uuid",
  headers = null,
  keysToExtract = null,
  columnMapping = null,
  sheetName = null
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

  // Read IDs from the CSV or Excel file
  let dfInput;
  try {
    dfInput = await readInputFile(inputFilePath, sheetName);
    if (!dfInput.some((row) => row.hasOwnProperty(idColumn))) {
      throw new Error(`Column '${idColumn}' not found in the input file`);
    }
  } catch (error) {
    console.log(`Error reading input file: ${error.message}`);
    return;
  }

  // Request configuration
  const REQUEST_DELAY = 200; // Delay between requests in milliseconds

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
      await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY));

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
      // const rawOutputPath = path.join(
      //   path.dirname(outputExcelPath),
      //   path.basename(outputExcelPath, path.extname(outputExcelPath)) +
      //     "_raw.xlsx"
      // );

      // const rawWb = XLSX.utils.book_new();
      // const rawWs = XLSX.utils.json_to_sheet(
      //   rawResponses.map((resp) => ({ raw_response: JSON.stringify(resp) }))
      // );
      // XLSX.utils.book_append_sheet(rawWb, rawWs, "Raw Responses");
      // XLSX.writeFile(rawWb, rawOutputPath);
      // console.log(`Raw responses saved to ${rawOutputPath} for reference`);
    } catch (error) {
      console.log(`Error saving data to Excel: ${error.message}`);
    }
  } else {
    console.log("No data was extracted from the API responses");
  }
}

/**
 * Helper function to determine file type and read accordingly
 * @param {string} filePath - Path to input file (CSV or Excel)
 * @param {string} sheetName - Name of Excel sheet to read (optional)
 * @returns {Promise<Array>} Array of objects representing file rows
 */
async function readInputFile(filePath, sheetName = null) {
  const fileExtension = path.extname(filePath).toLowerCase();

  if (fileExtension === ".csv") {
    console.log("Reading CSV file...");
    return await readCsv(filePath);
  } else if (fileExtension === ".xlsx" || fileExtension === ".xls") {
    console.log("Reading Excel file...");
    return await readExcel(filePath, sheetName);
  } else {
    throw new Error(
      `Unsupported file type: ${fileExtension}. Only .csv, .xlsx, and .xls files are supported.`
    );
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

/**
 * Helper function to read Excel file and return array of objects
 * @param {string} filePath - Path to Excel file
 * @param {string} sheetName - Name of the sheet to read (optional)
 * @returns {Promise<Array>} Array of objects representing Excel rows
 */
function readExcel(filePath, sheetName = null) {
  return new Promise((resolve, reject) => {
    try {
      // Read the Excel file
      const workbook = XLSX.readFile(filePath);

      // Get sheet name - use provided name or first sheet
      let targetSheetName;
      if (sheetName) {
        if (!workbook.SheetNames.includes(sheetName)) {
          throw new Error(
            `Sheet '${sheetName}' not found. Available sheets: ${workbook.SheetNames.join(
              ", "
            )}`
          );
        }
        targetSheetName = sheetName;
      } else {
        targetSheetName = workbook.SheetNames[0];
      }

      console.log(`Reading from sheet: ${targetSheetName}`);

      // Get the worksheet
      const worksheet = workbook.Sheets[targetSheetName];

      // Convert to JSON
      const jsonData = XLSX.utils.sheet_to_json(worksheet);

      // Clean up the data - remove empty rows and trim whitespace from headers
      const cleanedData = jsonData
        .filter((row) => Object.keys(row).length > 0) // Remove completely empty rows
        .map((row) => {
          const cleanedRow = {};
          for (const [key, value] of Object.entries(row)) {
            // Trim whitespace from keys and convert to string for consistency
            const cleanedKey = String(key).trim();
            cleanedRow[cleanedKey] = value;
          }
          return cleanedRow;
        });

      resolve(cleanedData);
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Helper function to validate file type
 * @param {string} filePath - Path to input file
 * @returns {boolean} True if file type is supported
 */
function isValidFileType(filePath) {
  const validExtensions = [".csv", ".xlsx", ".xls"];
  const fileExtension = path.extname(filePath).toLowerCase();
  return validExtensions.includes(fileExtension);
}

/**
 * Display usage information
 */
function displayUsage() {
  console.log(`
Usage: node ${path.basename(
    __filename
  )} <inputFile> [outputExcelFile] [sheetName]

Arguments:
  inputFile        Required. Path to the input CSV or Excel file containing UUIDs
                   Supported formats: .csv, .xlsx, .xls
  outputExcelFile  Optional. Path for the output Excel file. 
                   If not provided, will be generated automatically in the same directory as input file
  sheetName        Optional. Name of the Excel sheet to read (only applies to Excel files).
                   If not provided, the first sheet will be used.

Examples:
  node ${path.basename(__filename)} ./data/input.csv
  node ${path.basename(__filename)} ./data/input.xlsx
  node ${path.basename(__filename)} ./data/input.xlsx ./output/result.xlsx
  node ${path.basename(
    __filename
  )} ./data/input.xlsx ./output/result.xlsx "Sheet1"
  node ${path.basename(__filename)} "/path/to/Query-Merc-20250807.csv"
  node ${path.basename(
    __filename
  )} "/path/to/Query-Merc-20250807.xlsx" "/path/to/ExtractRes-Merc.xlsx" "Data"
  `);
}

// Main execution
async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2);

  // Check if help is requested
  if (args.includes("-h") || args.includes("--help") || args.length === 0) {
    displayUsage();
    return;
  }

  // Validate arguments
  if (args.length < 1) {
    console.error("Error: Input file path is required");
    displayUsage();
    process.exit(1);
  }

  const inputFile = args[0];

  // Check if input file exists
  if (!fs.existsSync(inputFile)) {
    console.error(`Error: Input file '${inputFile}' does not exist`);
    process.exit(1);
  }

  // Validate file type
  if (!isValidFileType(inputFile)) {
    console.error(
      `Error: Unsupported file type. Only .csv, .xlsx, and .xls files are supported.`
    );
    console.error(`Provided file: ${inputFile}`);
    process.exit(1);
  }

  // Generate output file path if not provided
  let outputExcelFile;
  if (args.length >= 2) {
    outputExcelFile = args[1];
  } else {
    // Auto-generate output file name in the same directory as input
    const inputDir = path.dirname(inputFile);
    const timestamp = new Date()
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\..+/, "")
      .replace("T", "-");
    outputExcelFile = path.join(inputDir, `APIData-${timestamp}.xlsx`);
  }

  // Get sheet name if provided (only relevant for Excel files)
  const sheetName = args.length >= 3 ? args[2] : null;

  // If sheet name is provided but input is CSV, show warning
  if (sheetName && path.extname(inputFile).toLowerCase() === ".csv") {
    console.warn("Warning: Sheet name parameter is ignored for CSV files");
  }

  // Ensure output directory exists
  const outputDir = path.dirname(outputExcelFile);
  if (!fs.existsSync(outputDir)) {
    try {
      fs.mkdirSync(outputDir, { recursive: true });
    } catch (error) {
      console.error(
        `Error creating output directory '${outputDir}': ${error.message}`
      );
      process.exit(1);
    }
  }

  console.log(`Input file: ${inputFile}`);
  console.log(`Output Excel file: ${outputExcelFile}`);
  if (sheetName && path.extname(inputFile).toLowerCase() !== ".csv") {
    console.log(`Target sheet: ${sheetName}`);
  }

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
    "data.transaccion.datos_pago.creacion",
    "data.transaccion.datos_procesador.data.all.data.datetime",
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
    "data.transaccion.datos_antifraude.tag_profile[0]",
    "data.transaccion.datos_antifraude.procesador",
    "data.transaccion.datos_procesador.data.all.data.data.numero_autorizacion",
    "data.transaccion.datos_procesador.data.all.codigo",
    "data.transaccion.datos_procesador.data.all.tipo_transaccion",
    "data.transaccion.datos_procesador.data.codigo",
    "data.transaccion.datos_procesador.data.mensaje",
    "data.transaccion.datos_antifraude.datos_procesador[0].descripcion",
    "data.transaccion.afiliacion_uuid",
    "data.transaccion.datos_procesador.numero_afiliacion",
    "data.transaccion.datos_procesador.procesador",
    "data.transaccion.datos_procesador.conciliaciones[0].id",
    "data.transaccion.datos_procesador.conciliaciones[0].fecha",
    "data.transaccion.datos_procesador.conciliaciones[0].nombre_retorno",
    "data.transaccion.comercio_uuid",
    "data.transaccion.datos_claropagos.origin",
    "data.transaccion.datos_comercio.cliente.uuid",
    "data.transaccion.datos_comercio.cliente.direccion.telefono.numero",
    "data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto",
    "data.transaccion.conciliado",
    "data.transaccion.fecha_conciliacion",
    "data.transaccion.datos_antifraude.datos_procesador[0].data.afsReply.cardScheme",
    "data.transaccion.updated_at",
  ];

  // Define column mapping to simplify header names
  const columnMapping = {
    "data.transaccion.uuid": "ID Transaccion",
    "data.transaccion.estatus": "Estado de Operacion",
    "data.transaccion.datos_pago.creacion": "Fecha",
    "data.transaccion.datos_procesador.data.all.data.datetime": "Fecha Captura",
    "data.transaccion.datos_comercio.pedido.id_externo": "Id Externo/Pedido",
    "data.transaccion.forma_pago": "Forma_Pago",
    "data.transaccion.datos_pago.nombre": "Nombre Tarjethabiente",
    "data.transaccion.datos_pago.pan": "Pan",
    "data.transaccion.datos_pago.marca": "Marca Tarjeta",
    "data.transaccion.monto": "Monto",
    "data.transaccion.moneda": "Moneda",
    "data.transaccion.pais": "Pais",
    "data.transaccion.datos_procesador.data.all.data.orderId": "ID Orden",
    "data.transaccion.datos_pago.plan_pagos.plan": "Tipo de plan de pagos",
    "data.transaccion.datos_pago.plan_pagos.diferido": "Diferimiento",
    "data.transaccion.datos_pago.plan_pagos.parcialidades": "Mensualidades",
    "data.transaccion.datos_pago.plan_pagos.puntos": "Puntos",
    "data.transaccion.origen": "Origen de Transaccion",
    "data.transaccion.operacion": "Esquema",
    "data.transaccion.datos_antifraude.resultado": "Resultado Antifraude",
    "data.transaccion.datos_antifraude.tag_profile[0]": "Perfil",
    "data.transaccion.datos_antifraude.procesador": "Procesador Antifraude",
    "data.transaccion.datos_procesador.data.all.data.data.numero_autorizacion":
      "Codigo de Autorizacion",
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
    "data.transaccion.datos_procesador.conciliaciones[0].id": "Id Conciliacion",
    "data.transaccion.datos_procesador.conciliaciones[0].fecha":
      "Fecha Conciliacion",
    "data.transaccion.datos_procesador.conciliaciones[0].nombre_retorno":
      "Archivo de Conciliacion",
    "data.transaccion.comercio_uuid": "Id Comercio",
    "data.transaccion.datos_claropagos.origin": "Nombre Comercio",
    "data.transaccion.datos_comercio.cliente.uuid": "Id Cliente",
    "data.transaccion.datos_comercio.cliente.direccion.telefono.numero":
      "Num Telf",
    "data.transaccion.datos_comercio.pedido.articulos[0].nombre_producto":
      "Id Producto",
    "data.transaccion.conciliado": "Cargo Conciliado",
    "data.transaccion.fecha_conciliacion": "Fecha Conciliacion",
    "data.transaccion.datos_antifraude.datos_procesador[0].data.afsReply.cardScheme":
      "Tipo Tarjeta",
    "data.transaccion.updated_at": "Fecha Actualizacion",
  };

  // Run the function
  await apiRequestWithExtraction(
    inputFile,
    outputExcelFile,
    urlTemplate,
    "uuid",
    headers,
    keysToExtract,
    columnMapping,
    sheetName
  );
}

// Execute main function if this file is run directly
if (require.main === module) {
  main().catch(console.error);
}
