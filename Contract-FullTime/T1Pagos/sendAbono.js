const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
require("dotenv").config();

// Environment variables
const apiToken = process.env.api_token;
const hiddenUrl = process.env.APISendAbono_URL;

// Fixed values that should not be modified
const FIXED_VALUES = {
  empresa: "CLARO_PAGOS",
  tipoPago: "1", // Kept as string as per example
  tipoCuentaOrdenante: 40,
  tipoCuentaBeneficiario: 40,
  institucionBeneficiaria: 90646,
};

// Function to convert string to appropriate type
function convertValue(key, value) {
  if (key === "id") {
    return value; // Skip id as it's not part of the payload
  }

  // Handle various data types - preserving strings that should be strings
  if (
    [
      "id",
      "referenciaNumerica",
      "tipoCuentaOrdenante",
      "institucionOrdenante",
      "tipoCuentaBeneficiario",
      "institucionBeneficiaria",
    ].includes(key)
  ) {
    try {
      return value ? parseInt(value) : 0;
    } catch (error) {
      return 0;
    }
  } else if (key === "monto") {
    try {
      return value ? parseFloat(value) : 0.0;
    } catch (error) {
      return 0.0;
    }
  }
  // Preserve these fields as strings to maintain leading zeros
  else if (
    [
      "claveRastreo",
      "conceptoPago",
      "fechaOperacion",
      "cuentaOrdenante",
      "rfcCurpOrdenante",
      "cuentaBeneficiario",
    ].includes(key)
  ) {
    return String(value);
  } else {
    return value;
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

// Function to write CSV file
async function writeCsvFile(filePath, data) {
  if (data.length === 0) {
    console.log("No data to write to CSV file.");
    return;
  }

  // Determine all possible keys across all responses
  const allKeys = new Set();
  data.forEach((item) => {
    Object.keys(item).forEach((key) => allKeys.add(key));
  });

  const csvWriter = createCsvWriter({
    path: filePath,
    header: Array.from(allKeys).map((key) => ({ id: key, title: key })),
  });

  try {
    await csvWriter.writeRecords(data);
    console.log(`Successfully wrote ${data.length} responses to ${filePath}`);
  } catch (error) {
    console.error(`Error writing CSV file: ${error}`);
  }
}

/**
 * Display usage information
 */
function displayUsage() {
  console.log(`
Usage: node ${path.basename(__filename)} <inputCsvFile> [outputCsvFile]

Arguments:
  inputCsvFile     Required. Path to the input CSV file containing payment data
                   Must contain an 'id' column and payment-related fields
  outputCsvFile    Optional. Path for the output CSV file with API responses. 
                   If not provided, will be generated automatically in the same directory as input file

Examples:
  node ${path.basename(__filename)} ./data/payments.csv
  node ${path.basename(__filename)} ./data/payments.csv ./output/responses.csv
  node ${path.basename(__filename)} "/path/to/SendAbono_20250708.csv"
  node ${path.basename(
    __filename
  )} "/path/to/input.csv" "/path/to/SendAbonoResponses.csv"

Required CSV Columns:
  - id: Identifier for tracking requests
  - Payment fields like: monto, claveRastreo, conceptoPago, fechaOperacion, etc.

Environment Variables Required:
  - api_token: Your API authorization token
  - APISendAbono_URL: The URL for the send abono API endpoint

Fixed Values (automatically applied):
  - empresa: "CLARO_PAGOS"
  - tipoPago: "1"
  - tipoCuentaOrdenante: 40
  - tipoCuentaBeneficiario: 40
  - institucionBeneficiaria: 90646
  `);
}

/**
 * Process API send abono requests from CSV data
 * @param {string} inputCsvFile - Path to input CSV file
 * @param {string} outputCsvFile - Path to output CSV file
 */
async function processApiSendAbono(inputCsvFile, outputCsvFile) {
  try {
    // Read data from CSV file
    const rows = await readCsvFile(inputCsvFile);
    const totalRows = rows.length;

    if (totalRows === 0) {
      console.log("No data found in the input CSV file.");
      return;
    }

    // Check if id column exists
    if (!rows[0].hasOwnProperty("id")) {
      console.error("Error: 'id' column not found in the input CSV file");
      process.exit(1);
    }

    // List storing all API responses and errors
    const allData = [];

    console.log(`Processing ${totalRows} rows from CSV file...`);

    // Process each row in CSV file
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];

      // Create payload from row data
      const payload = {};

      // Convert types appropriately
      for (const [key, value] of Object.entries(row)) {
        if (key === "id") {
          continue; // Skip id as it's not part of the payload
        }

        payload[key] = convertValue(key, value);
      }

      // Apply fixed values, overriding any from the CSV
      Object.assign(payload, FIXED_VALUES);

      // Get the ID for the request URL
      const idValue = row.id || "";
      const requestUrl = hiddenUrl;

      console.log(`[${index + 1}/${totalRows}] Sending to URL: ${requestUrl}`);
      console.log(`Payload: ${JSON.stringify(payload, null, 2)}`);

      try {
        const response = await axios.post(requestUrl, payload, { headers });
        const responseData = response.data || {};

        if (response.status === 200) {
          responseData.id = idValue; // Add ID to response
          responseData.error = ""; // Empty column if no error is returned
          responseData.request_status = "success";
          responseData.status_code = response.status;
          allData.push(responseData);
        } else {
          const errorMessage = responseData.error || response.statusText;
          console.log(
            `Error on ID ${idValue}: ${response.status} - ${errorMessage}`
          );
          allData.push({
            id: idValue,
            error: errorMessage,
            request_status: "failed",
            status_code: response.status,
          });
        }
      } catch (error) {
        let errorMessage;
        let statusCode = "Exception";

        if (error.response) {
          // The request was made and the server responded with a status code
          errorMessage =
            error.response.data?.error || error.response.statusText;
          statusCode = error.response.status;
          console.log(
            `Error on ID ${idValue}: ${error.response.status} - ${errorMessage}`
          );
        } else if (error.request) {
          // The request was made but no response was received
          errorMessage = `Connection error: ${error.message}`;
          console.log(`Connection error on ID ${idValue}: ${error.message}`);
        } else {
          // Something happened in setting up the request
          errorMessage = error.message;
          console.log(`Request setup error on ID ${idValue}: ${error.message}`);
        }

        allData.push({
          id: idValue,
          error: errorMessage,
          request_status: "failed",
          status_code: statusCode,
        });
      }
    }

    // Save all responses to CSV file
    await writeCsvFile(outputCsvFile, allData);

    // Print summary
    const successfulRequests = allData.filter(
      (item) => item.request_status === "success"
    ).length;
    const failedRequests = allData.length - successfulRequests;
    console.log(
      `\nSummary: ${successfulRequests} successful, ${failedRequests} failed out of ${allData.length} total requests`
    );
  } catch (error) {
    console.error(`Error reading CSV file: ${error.message}`);
  }
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
    console.error("Error: Input CSV file path is required");
    displayUsage();
    process.exit(1);
  }

  const inputCsvFile = args[0];

  // Check if input file exists
  if (!fs.existsSync(inputCsvFile)) {
    console.error(`Error: Input file '${inputCsvFile}' does not exist`);
    process.exit(1);
  }

  // Generate output file path if not provided
  let outputCsvFile;
  if (args.length >= 2) {
    outputCsvFile = args[1];
  } else {
    // Auto-generate output file name in the same directory as input
    const inputDir = path.dirname(inputCsvFile);
    const inputBasename = path.basename(
      inputCsvFile,
      path.extname(inputCsvFile)
    );
    const timestamp = new Date()
      .toISOString()
      .replace(/[-T:]/g, "")
      .split(".")[0];
    outputCsvFile = path.join(
      inputDir,
      `SendAbonoResponses-${inputBasename}-${timestamp}.csv`
    );
  }

  // Ensure output directory exists
  const outputDir = path.dirname(outputCsvFile);
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

  // Check for required environment variables
  if (!apiToken) {
    console.error(
      "Error: API token not found. Make sure api_token is set in your .env file"
    );
    process.exit(1);
  }

  if (!hiddenUrl) {
    console.error(
      "Error: API URL not found. Make sure APISendAbono_URL is set in your .env file"
    );
    process.exit(1);
  }

  console.log(`Input CSV file: ${inputCsvFile}`);
  console.log(`Output CSV file: ${outputCsvFile}`);
  console.log(`API endpoint: ${hiddenUrl}`);

  // Run the send abono process
  await processApiSendAbono(inputCsvFile, outputCsvFile);
}

// Execute main function if this file is run directly
if (require.main === module) {
  main().catch(console.error);
}
