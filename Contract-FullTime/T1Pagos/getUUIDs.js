const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const XLSX = require("xlsx");
require("dotenv").config();

// Environment variables
const apiToken = process.env.SearsAdmin_Token;
const hiddenUrl = process.env.externalId_URL;

/**
 * Make API requests based on IDs from a CSV file, extract specified key-value pairs,
 * and save the results directly to Excel.
 *
 * @param {string} inputCsvPath - Path to the input CSV file containing IDs
 * @param {string} outputExcelPath - Path where the output Excel file will be saved
 * @param {string} urlTemplate - API endpoint URL template (ID will be appended)
 * @param {string} idColumn - Name of the column in the CSV containing the IDs (default: 'Id Externo')
 * @param {Object} headers - Headers for the API request including authorization
 * @param {Array} keysToExtract - List of keys to extract from the API response. If empty, all keys will be included.
 * @param {Object} columnMapping - Dictionary mapping original key paths to desired column names
 */
async function apiRequestWithExtraction(
  inputCsvPath,
  outputExcelPath,
  urlTemplate,
  idColumn = "Id Externo",
  headers = null,
  keysToExtract = null,
  columnMapping = null
) {
  if (headers === null) {
    headers = {
      Authorization: "Bearer ",
      "Content-Type": "application/json",
    };
  }

  if (keysToExtract === null) {
    keysToExtract = [];
  }

  if (columnMapping === null) {
    columnMapping = {};
  }

  // Read IDs from the CSV file
  let dfInput;
  try {
    dfInput = await readCsvFile(inputCsvPath);

    // Check if required column exists
    if (!dfInput.length || !dfInput[0].hasOwnProperty(idColumn)) {
      throw new Error(`Column '${idColumn}' not found in the input CSV file`);
    }
  } catch (error) {
    console.error(`Error reading input CSV: ${error.message}`);
    return;
  }

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
    const url = `${urlTemplate}${idValue}/pedido`;

    console.log(`[${index + 1}/${totalIds}] Requesting URL: ${url}`);

    try {
      // Perform GET request
      const response = await axios.get(url, { headers });

      if (response.status === 200) {
        const data = response.data; // JSON response
        rawResponses.push(data); // Store raw response

        // Extract selected key-value pairs or all keys if none specified
        const extractedItem = {};
        extractedItem[idColumn] = idValue; // Always include the ID

        // Function to recursively search for keys in nested dictionaries
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
        // Add a row with error information
        extractedData.push({
          [idColumn]: idValue,
          error: `Status code ${response.status}`,
        });
      }
    } catch (error) {
      console.log(`Exception for ID ${idValue}: ${error.message}`);

      let errorMessage = error.message;
      if (error.response) {
        errorMessage = `Status code ${error.response.status}`;
      }

      extractedData.push({
        [idColumn]: idValue,
        error: errorMessage,
      });
    }
  }

  // Convert to DataFrame and save to Excel
  if (extractedData.length > 0) {
    try {
      // Apply column mapping if provided
      const mappedData = extractedData.map((item) => {
        const mappedItem = {};
        for (const [key, value] of Object.entries(item)) {
          const mappedKey = columnMapping[key] || key;
          mappedItem[mappedKey] = value;
        }
        return mappedItem;
      });

      // Create workbook and worksheet
      const workbook = XLSX.utils.book_new();
      const worksheet = XLSX.utils.json_to_sheet(mappedData);

      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(workbook, worksheet, "Extracted Data");

      // Save to Excel
      XLSX.writeFile(workbook, outputExcelPath);
      console.log(
        `Data successfully extracted and saved to ${outputExcelPath}`
      );

      // Also save raw responses for debugging or further processing
      const rawOutputPath = outputExcelPath.replace(/\.xlsx?$/i, "_raw.xlsx");
      const rawWorkbook = XLSX.utils.book_new();
      const rawData = rawResponses.map((resp, index) => ({
        row_number: index + 1,
        id: dfInput[index][idColumn],
        raw_response: JSON.stringify(resp, null, 2),
      }));
      const rawWorksheet = XLSX.utils.json_to_sheet(rawData);
      XLSX.utils.book_append_sheet(rawWorkbook, rawWorksheet, "Raw Responses");
      XLSX.writeFile(rawWorkbook, rawOutputPath);
      console.log(`Raw responses saved to ${rawOutputPath} for reference`);
    } catch (error) {
      console.error(`Error saving data to Excel: ${error.message}`);
    }
  } else {
    console.log("No data was extracted from the API responses");
  }
}

// Function to read CSV file
function readCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", reject);
  });
}

// Main execution
async function main() {
  try {
    // Configuration - update these values as needed
    const inputCsvFile = path.join(
      process.env.HOME || process.env.USERPROFILE,
      "Downloads",
      "IdTrx-Sears-20250704.csv"
    );
    const timestamp =
      new Date().toISOString().replace(/[-T:]/g, "").split(".")[0] +
      String(new Date().getMonth() + 1).padStart(2, "0");
    const outputExcelFile = path.join(
      process.env.HOME || process.env.USERPROFILE,
      "Downloads",
      `UUIDs-Sears-${timestamp}.xlsx`
    );

    // API endpoint - choose the one you need
    const urlTemplate = hiddenUrl;

    // Headers with authorization token
    const headers = {
      Authorization: `Bearer ${apiToken}`,
      "Content-Type": "application/json",
    };

    // Keys to extract - add or remove keys as needed
    // For nested keys, use dot notation, e.g., 'customer.id_externo'
    const keysToExtract = [
      "data.transaccion.cargo_id",
      "data.transaccion.monto",
      "data.transaccion.afiliacion",
      "data.transaccion.autorizacion_id",
      "data.transaccion.conciliado",
      "data.transaccion.fecha_creacion",
      "data.transaccion.procesador",
      "data.transaccion.tipo_operacion",
      // Add any other keys you need
    ];

    // Define column mapping to simplify header names
    // Format: 'original_key_path': 'desired_column_name'
    const columnMapping = {
      "data.transaccion.cargo_id": "Id Transaccion",
      "data.transaccion.monto": "Monto",
      "data.transaccion.afiliacion": "Id Afiliacion",
      "data.transaccion.autorizacion_id": "Autorizacion",
      "data.transaccion.conciliado": "¿Conciliado?",
      "data.transaccion.fecha_creacion": "Fecha",
      "data.transaccion.procesador": "Procesador",
      "data.transaccion.tipo_operacion": "Tipo de Operacion",
      // Add mappings for other keys as needed
    };

    // Run the combined function
    await apiRequestWithExtraction(
      inputCsvFile,
      outputExcelFile,
      urlTemplate,
      "Id Externo",
      headers,
      keysToExtract,
      columnMapping
    );
  } catch (error) {
    console.error(`Main execution error: ${error.message}`);
  }
}

// Run the main function
if (require.main === module) {
  main().catch(console.error);
}

// Export the function for use as a module
module.exports = { apiRequestWithExtraction };
