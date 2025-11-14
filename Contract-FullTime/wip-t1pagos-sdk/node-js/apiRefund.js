const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const XLSX = require("xlsx");
require("dotenv").config();

// Environment variables
const apiToken = process.env.api_token;
const baseUrl = process.env.APIrefundBaseURL;

/**
 * Make refund API requests based on cargo IDs and amounts from a CSV file,
 * and save the results to Excel.
 *
 * @param {string} inputCsvPath - Path to the input CSV file containing cargo IDs and amounts
 * @param {string} outputExcelPath - Path where the output Excel file will be saved
 * @param {string} baseUrl - Base API URL (without the endpoint path)
 * @param {string} cargoIdColumn - Name of the column in the CSV containing the cargo IDs (default: 'idCargo')
 * @param {string} amountColumn - Name of the column in the CSV containing the refund amounts (default: 'montoTotal')
 * @param {Object} headers - Headers for the API request including authorization
 */
async function apiRefundRequests(
  inputCsvPath,
  outputExcelPath,
  baseUrl,
  cargoIdColumn = "idCargo",
  amountColumn = "montoTotal",
  headers = null
) {
  if (headers === null) {
    headers = {
      Authorization: "Bearer ",
      "Content-Type": "application/json",
    };
  }

  // Read data from the CSV file
  let dfInput;
  try {
    dfInput = await readCsvFile(inputCsvPath);

    // Check if required columns exist
    if (!dfInput.length || !dfInput[0].hasOwnProperty(cargoIdColumn)) {
      throw new Error(
        `Column '${cargoIdColumn}' not found in the input CSV file`
      );
    }
    if (!dfInput[0].hasOwnProperty(amountColumn)) {
      throw new Error(
        `Column '${amountColumn}' not found in the input CSV file`
      );
    }
  } catch (error) {
    console.error(`Error reading input CSV: ${error.message}`);
    return;
  }

  // List to store all results
  const resultsData = [];

  // Process each row
  const totalRows = dfInput.length;
  for (let index = 0; index < dfInput.length; index++) {
    const row = dfInput[index];
    const cargoId = String(row[cargoIdColumn]);
    const montoTotal = row[amountColumn];

    // Construct the refund endpoint URL
    const url = `${baseUrl}/v1/cargo/${cargoId}/reembolsar`;

    // Prepare the request body
    const requestBody = {
      monto: montoTotal,
    };

    console.log(
      `[${
        index + 1
      }/${totalRows}] Requesting refund for cargo ID: ${cargoId}, amount: ${montoTotal}`
    );
    console.log(`URL: ${url}`);
    console.log(`Body: ${JSON.stringify(requestBody)}`);

    let resultItem = {
      [cargoIdColumn]: cargoId,
      [amountColumn]: montoTotal,
      request_url: url,
      request_timestamp: new Date().toISOString(),
    };

    try {
      // Perform POST request
      const response = await axios.post(url, requestBody, { headers });

      resultItem.status_code = response.status;

      if (response.status === 200) {
        try {
          const responseData = response.data;
          resultItem.success = true;
          resultItem.response_message = "Success";

          // Add key response fields if they exist
          if (typeof responseData === "object" && responseData !== null) {
            // Add common response fields - adjust these based on your API response structure
            resultItem.response_id = responseData.id || "";
            resultItem.response_status = responseData.status || "";
            resultItem.response_message_detail = responseData.message || "";
            resultItem.transaction_id = responseData.transaction_id || "";

            // Store the full response as a string for reference
            resultItem.full_response = JSON.stringify(responseData);
          } else {
            resultItem.full_response = String(responseData);
          }
        } catch (jsonError) {
          resultItem.success = true; // HTTP 200 but JSON parsing failed
          resultItem.response_message = `Success (JSON parse error: ${jsonError.message})`;
          resultItem.full_response = response.data;
        }
      } else {
        resultItem.success = false;
        resultItem.response_message = `HTTP Error ${response.status}`;
        resultItem.full_response = response.data;

        console.log(
          `Error for cargo ID ${cargoId}: Status code ${response.status}`
        );
      }
    } catch (error) {
      console.log(`Exception for cargo ID ${cargoId}: ${error.message}`);

      if (error.response) {
        // The request was made and the server responded with a status code
        resultItem.status_code = error.response.status;
        resultItem.success = false;
        resultItem.response_message = `HTTP Error ${error.response.status}`;

        try {
          const errorData = error.response.data;
          resultItem.error_detail = JSON.stringify(errorData);
        } catch {
          resultItem.error_detail = error.response.data;
        }
        resultItem.full_response = error.response.data;
      } else if (error.request) {
        // The request was made but no response was received
        resultItem.status_code = "Exception";
        resultItem.success = false;
        resultItem.response_message = `Request Exception: ${error.message}`;
        resultItem.error_detail = error.message;
      } else {
        // Something happened in setting up the request
        resultItem.status_code = "Exception";
        resultItem.success = false;
        resultItem.response_message = `Request Exception: ${error.message}`;
        resultItem.error_detail = error.message;
      }
    }

    resultsData.push(resultItem);
  }

  // Save results to Excel
  if (resultsData.length > 0) {
    try {
      // Define column order for better readability
      const columnOrder = [
        cargoIdColumn,
        amountColumn,
        "success",
        "status_code",
        "response_message",
        "request_timestamp",
        "request_url",
      ];

      // Add other columns that might exist
      const allColumns = new Set();
      resultsData.forEach((item) => {
        Object.keys(item).forEach((key) => allColumns.add(key));
      });

      const remainingColumns = Array.from(allColumns).filter(
        (col) => !columnOrder.includes(col)
      );
      const finalColumnOrder = [...columnOrder, ...remainingColumns];

      // Reorder data according to column order
      const orderedData = resultsData.map((item) => {
        const orderedItem = {};
        finalColumnOrder.forEach((col) => {
          orderedItem[col] = item[col] || "";
        });
        return orderedItem;
      });

      // Create workbook and worksheet
      const workbook = XLSX.utils.book_new();
      const worksheet = XLSX.utils.json_to_sheet(orderedData);

      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(workbook, worksheet, "Refund Results");

      // Save to Excel file
      XLSX.writeFile(workbook, outputExcelPath);
      console.log(`\nResults successfully saved to ${outputExcelPath}`);

      // Print summary
      const successfulRequests = resultsData.filter(
        (item) => item.success
      ).length;
      const failedRequests = resultsData.length - successfulRequests;
      console.log(
        `Summary: ${successfulRequests} successful, ${failedRequests} failed out of ${resultsData.length} total requests`
      );
    } catch (error) {
      console.error(`Error saving results to Excel: ${error.message}`);
    }
  } else {
    console.log("No results to save");
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
 * Display usage information
 */
function displayUsage() {
  console.log(`
Usage: node ${path.basename(__filename)} <inputCsvFile> [outputExcelFile]

Arguments:
  inputCsvFile     Required. Path to the input CSV file containing cargo IDs and amounts
                   Must contain columns: 'idCargo' and 'montoTotal'
  outputExcelFile  Optional. Path for the output Excel file. 
                   If not provided, will be generated automatically in the same directory as input file

Examples:
  node ${path.basename(__filename)} ./data/refunds.csv
  node ${path.basename(__filename)} ./data/refunds.csv ./output/results.xlsx
  node ${path.basename(__filename)} "/path/to/RyP-Refund-20250803.csv"
  node ${path.basename(
    __filename
  )} "/path/to/input.csv" "/path/to/RefundResults.xlsx"

Required CSV Columns:
  - idCargo: The cargo ID for the refund request
  - montoTotal: The refund amount

Environment Variables Required:
  - api_token: Your API authorization token
  - APIrefundBaseURL: The base URL for the refund API
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
  let outputExcelFile;
  if (args.length >= 2) {
    outputExcelFile = args[1];
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
    outputExcelFile = path.join(
      inputDir,
      `RefundResults-${inputBasename}-${timestamp}.xlsx`
    );
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

  // Check for required environment variables
  if (!apiToken) {
    console.error(
      "Error: API token not found. Make sure api_token is set in your .env file"
    );
    process.exit(1);
  }

  if (!baseUrl) {
    console.error(
      "Error: API base URL not found. Make sure APIrefundBaseURL is set in your .env file"
    );
    process.exit(1);
  }

  console.log(`Input CSV file: ${inputCsvFile}`);
  console.log(`Output Excel file: ${outputExcelFile}`);

  // Headers with authorization token
  const headers = {
    Authorization: `Bearer ${apiToken}`,
    "Content-Type": "application/json",
  };

  // Run the refund function
  await apiRefundRequests(
    inputCsvFile,
    outputExcelFile,
    baseUrl,
    "idCargo", // Change this if your CSV column has a different name
    "montoTotal", // Change this if your CSV column has a different name
    headers
  );
}

// Execute main function if this file is run directly
if (require.main === module) {
  main().catch(console.error);
}

// Export the function for use as a module
module.exports = { apiRefundRequests };
