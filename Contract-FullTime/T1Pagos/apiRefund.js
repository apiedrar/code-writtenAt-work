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

// Main execution
async function main() {
  try {
    // Configuration - update these values as needed
    const inputCsvFile = path.join(
      process.env.HOME || process.env.USERPROFILE,
      "Downloads",
      "RyP-Refund-20250803.csv"
    );
    const timestamp = new Date()
      .toISOString()
      .replace(/[-T:]/g, "")
      .split(".")[0];
    const outputExcelFile = path.join(
      process.env.HOME || process.env.USERPROFILE,
      "Downloads",
      `Results-RyP-Refund-${timestamp}.xlsx`
    );

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
  } catch (error) {
    console.error(`Main execution error: ${error.message}`);
  }
}

// Run the main function
if (require.main === module) {
  main().catch(console.error);
}

// Export the function for use as a module
module.exports = { apiRefundRequests };
