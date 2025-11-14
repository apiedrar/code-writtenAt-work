const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const XLSX = require("xlsx");
require("dotenv").config();

// Environment variables
const apiToken = process.env.claroPayAdmin_token; // Update "Merc" to the specific merchant's name, or assign the variable to the correct value in your .env file
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
    const url = `${urlTemplate}${idValue}`;

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

/**
 * Parse command line arguments
 * @returns {Object} Parsed arguments
 */
function parseArguments() {
  const args = process.argv.slice(2);
  const parsedArgs = {
    input: null,
    output: null,
    idColumn: "Id Externo",
    merchant: "Merc",
    help: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];

    switch (arg) {
      case "-i":
      case "--input":
        if (nextArg && !nextArg.startsWith("-")) {
          parsedArgs.input = nextArg;
          i++; // Skip next argument as it's the value
        }
        break;
      case "-o":
      case "--output":
        if (nextArg && !nextArg.startsWith("-")) {
          parsedArgs.output = nextArg;
          i++; // Skip next argument as it's the value
        }
        break;
      case "-c":
      case "--id-column":
        if (nextArg && !nextArg.startsWith("-")) {
          parsedArgs.idColumn = nextArg;
          i++; // Skip next argument as it's the value
        }
        break;
      case "-m":
      case "--merchant":
        if (nextArg && !nextArg.startsWith("-")) {
          parsedArgs.merchant = nextArg;
          i++; // Skip next argument as it's the value
        }
        break;
      case "-h":
      case "--help":
        parsedArgs.help = true;
        break;
      default:
        console.warn(`Unknown argument: ${arg}`);
    }
  }

  return parsedArgs;
}

/**
 * Display help information
 */
function displayHelp() {
  console.log(`
Usage: node getUUIDs.js [options]

Options:
  -i, --input <path>        Path to input CSV file
  -o, --output <path>       Path to output Excel file
  -c, --id-column <name>    Name of the ID column in CSV (default: "Id Externo")
  -m, --merchant <name>     Merchant name for default file naming (default: "Merc")
  -h, --help                Display this help message

Examples:
  # Use default file locations with merchant name
  node getUUIDs.js

  # Specify custom input and output files
  node getUUIDs.js -i /path/to/input.csv -o /path/to/output.xlsx

  # Use custom ID column name
  node getUUIDs.js -i input.csv -c "External_ID"

  # Combine multiple options
  node getUUIDs.js -i data.csv -o results.xlsx -m "MyMerchant" -c "TransactionID"

Default behavior (no arguments):
  - Input: ~/Downloads/IdTrx-Merc-YYYYMMDD.csv
  - Output: ~/Downloads/UUIDs-Merc-{timestamp}.xlsx
  - ID Column: "Id Externo"
  - Merchant: "Merc"
`);
}

/**
 * Generate default file paths based on merchant name
 * @param {string} merchant - Merchant name
 * @returns {Object} Object with inputFile and outputFile paths
 */
function generateDefaultPaths(merchant) {
  const downloadsPath = process.env.HOME || process.env.USERPROFILE;
  const inputFile = path.join(
    downloadsPath,
    "Downloads",
    `IdTrx-${merchant}-20250704.csv`
  );

  const timestamp =
    new Date().toISOString().replace(/[-T:]/g, "").split(".")[0] +
    String(new Date().getMonth() + 1).padStart(2, "0");
  const outputFile = path.join(
    downloadsPath,
    "Downloads",
    `UUIDs-${merchant}-${timestamp}.xlsx`
  );

  return { inputFile, outputFile };
}

/**
 * Validate file paths and arguments
 * @param {Object} args - Parsed arguments
 * @returns {Object} Validated file paths
 */
function validateAndPreparePaths(args) {
  let inputFile, outputFile;

  // Handle input file
  if (args.input) {
    inputFile = path.resolve(args.input);
    if (!fs.existsSync(inputFile)) {
      throw new Error(`Input file does not exist: ${inputFile}`);
    }
  } else {
    // Use default path
    const defaultPaths = generateDefaultPaths(args.merchant);
    inputFile = defaultPaths.inputFile;
    if (!fs.existsSync(inputFile)) {
      throw new Error(
        `Default input file does not exist: ${inputFile}. Please specify input file with -i option.`
      );
    }
  }

  // Handle output file
  if (args.output) {
    outputFile = path.resolve(args.output);
    // Ensure output directory exists
    const outputDir = path.dirname(outputFile);
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
  } else {
    // Use default path
    const defaultPaths = generateDefaultPaths(args.merchant);
    outputFile = defaultPaths.outputFile;
    // Ensure output directory exists
    const outputDir = path.dirname(outputFile);
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
  }

  return { inputFile, outputFile };
}

// Main execution
async function main() {
  try {
    // Parse command line arguments
    const args = parseArguments();

    // Display help if requested
    if (args.help) {
      displayHelp();
      return;
    }

    console.log("Starting UUID extraction process...");
    console.log(`Merchant: ${args.merchant}`);
    console.log(`ID Column: ${args.idColumn}`);

    // Validate and prepare file paths
    const { inputFile, outputFile } = validateAndPreparePaths(args);

    console.log(`Input file: ${inputFile}`);
    console.log(`Output file: ${outputFile}`);

    // Verify environment variables
    if (!apiToken) {
      throw new Error("MercAdmin_Token environment variable is not set");
    }
    if (!hiddenUrl) {
      throw new Error("externalId_URL environment variable is not set");
    }

    // API endpoint
    const urlTemplate = hiddenUrl;

    // Headers with authorization token
    const headers = {
      Authorization: `Bearer ${apiToken}`,
      "Content-Type": "application/json",
    };

    // Keys to extract - add or remove keys as needed
    // For nested keys, use dot notation, e.g., 'customer.id_externo'
    const keysToExtract = [
      "data.cargo[0].id",
      "data.cargo[0].monto",
      "data.cargo[0].fecha",
      "data.cargo[0.orden_id",
      "data.cargo[0].metodo_pago",
      "data.cargo[0].estatus",
      "data.cargo[0].cliente.id",
      "data.cargo[0].cliente.email",
      "data.cargo[0].cliente.id_externo",
      "data.cargo[0].tarjeta.iin",
      "data.cargo[0].tarjeta.terminacion",
      "data.cargo[0].tarjeta.producto",
      // Add any other keys you need
    ];

    // Define column mapping to simplify header names
    // Format: 'original_key_path': 'desired_column_name'
    const columnMapping = {
      "data.cargo[0].id": "idCargo",
      "data.cargo[0].monto": "montoTotal",
      "data.cargo[0].fecha": "Fecha",
      "data.cargo[0.orden_id": "No. Externo/Pedido",
      "data.cargo[0].metodo_pago": "Metodo de Pago",
      "data.cargo[0].estatus": "Estado de Operación",
      "data.cargo[0].cliente.id": "ID Cliente",
      "data.cargo[0].cliente.email": "Email Cliente",
      "data.cargo[0].cliente.id_externo": "ID Externo Cliente",
      "data.cargo[0].tarjeta.iin": "BIN",
      "data.cargo[0].tarjeta.terminacion": "Terminación",
      "data.cargo[0].tarjeta.producto": "Tipo Tarjeta",
      // Add mappings for other keys as needed
    };

    // Run the combined function
    await apiRequestWithExtraction(
      inputFile,
      outputFile,
      urlTemplate,
      args.idColumn,
      headers,
      keysToExtract,
      columnMapping
    );

    console.log("Process completed successfully!");
  } catch (error) {
    console.error(`Main execution error: ${error.message}`);
    process.exit(1);
  }
}

// Run the main function only if this script is executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error(`Unhandled error: ${error.message}`);
    process.exit(1);
  });
}

// Export the function for use as a module
module.exports = {
  apiRequestWithExtraction,
  parseArguments,
  generateDefaultPaths,
};
