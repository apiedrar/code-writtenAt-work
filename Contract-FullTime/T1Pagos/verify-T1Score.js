#!/usr/bin/env node

/**
 * API Validation Script
 * Sends GET requests to validate emails/phone numbers against multiple lists
 * and outputs results to an Excel file.
 */

const fs = require("fs");
const path = require("path");
const os = require("os");
const axios = require("axios");
const XLSX = require("xlsx");
require("dotenv").config();

/**
 * Sleep function to add delay between requests
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Send GET request to validate a value against a specific list
 * @param {string} baseUrl - Base API URL
 * @param {string} listId - List identifier
 * @param {string} value - Value to validate (email or phone)
 * @param {number} timeout - Request timeout in milliseconds
 * @returns {Promise<boolean>} - True if found, False otherwise
 */
async function validateValue(baseUrl, listId, value, timeout = 30000) {
  try {
    // Construct the URL manually
    // Ensure base_url ends with proper format
    if (!baseUrl.endsWith("/")) {
      baseUrl = baseUrl + "/";
    }

    // Build the complete URL
    const url = `${baseUrl}v1/list/${listId}/items/${value}/verify`;

    // Set headers similar to Postman
    const headers = {
      "User-Agent": "NodeJS-Script/1.0",
      Accept: "*/*",
      "Accept-Encoding": "gzip, deflate, br",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    };

    // Debug: Print the constructed URL
    console.log(`    Requesting: ${url}`);

    // Make GET request
    const response = await axios.get(url, {
      headers,
      timeout,
    });

    // Check if request was successful
    if (response.status === 200) {
      const data = response.data;
      // Extract the 'found' boolean value
      return Boolean(data.found || false);
    } else {
      console.log(
        `  Warning: API request failed for ${listId}/${value} - Status: ${response.status}`
      );
      return false;
    }
  } catch (error) {
    if (error.code === "ECONNABORTED") {
      console.log(`  Warning: Request timeout for ${listId}/${value}`);
    } else if (error.response) {
      console.log(
        `  Warning: API request failed for ${listId}/${value} - Status: ${error.response.status}`
      );
    } else if (error.request) {
      console.log(
        `  Warning: Request error for ${listId}/${value}: ${error.message}`
      );
    } else {
      console.log(
        `  Warning: Unexpected error for ${listId}/${value}: ${error.message}`
      );
    }
    return false;
  }
}

/**
 * Main function
 */
async function main() {
  // Configuration
  const API_BASE_URL = process.env.dev_VerifyItem_URL; // Set this in your .env file
  const INPUT_FILE = path.join(
    os.homedir(),
    "Downloads",
    "Tests-20250603.xlsx"
  );
  const OUTPUT_FILE = path.join(
    os.homedir(),
    "Downloads",
    "Verify-20250603.xlsx"
  );

  // Array of IDs to iterate through - Parse from environment variable
  const idListStr = process.env.id_list || "";
  let ID_LIST = [];

  if (idListStr) {
    // Split by comma and strip whitespace
    ID_LIST = idListStr.split(",").map((item) => item.trim());
  } else {
    // Fallback array if env var not set
    ID_LIST = [];
  }

  // Request configuration
  const REQUEST_DELAY = 50; // Delay between requests in milliseconds
  const TIMEOUT = 30000; // Request timeout in milliseconds

  console.log("Starting API validation process...");
  console.log(`API Base URL: ${API_BASE_URL}`);
  console.log(`ID List: ${ID_LIST}`);
  console.log(`Number of lists to check: ${ID_LIST.length}`);

  if (!API_BASE_URL) {
    throw new Error(
      "API_BASE_URL not found in environment variables. Please check your .env file."
    );
  }

  try {
    // Read Excel file
    console.log(`Reading input file: ${INPUT_FILE}`);

    if (!fs.existsSync(INPUT_FILE)) {
      throw new Error(`Input file not found at ${INPUT_FILE}`);
    }

    const workbook = XLSX.readFile(INPUT_FILE);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);

    if (data.length === 0) {
      throw new Error("Excel file is empty");
    }

    // Validate required columns
    const requiredColumns = ["area_code", "phone_number", "email"];
    const availableColumns = Object.keys(data[0]);
    const missingColumns = requiredColumns.filter(
      (col) => !availableColumns.includes(col)
    );

    if (missingColumns.length > 0) {
      throw new Error(`Missing required columns: ${missingColumns.join(", ")}`);
    }

    // Create phone numbers by concatenating area_code and phone_number
    data.forEach((row) => {
      row.full_phone =
        String(row.area_code || "") + String(row.phone_number || "");
    });

    console.log(
      `Processing ${data.length} rows with ${ID_LIST.length} lists...`
    );

    // Initialize result columns for each list ID
    ID_LIST.forEach((listId) => {
      data.forEach((row) => {
        row[`${listId}_email`] = 0; // Default to 0 (false)
        row[`${listId}_phone`] = 0; // Default to 0 (false)
      });
    });

    // Process each row
    for (let index = 0; index < data.length; index++) {
      const row = data[index];
      const email = row.email;
      const phone = row.full_phone;

      console.log(`Processing row ${index + 1}/${data.length}`);

      // Process email validation for each list
      if (email && String(email).trim()) {
        for (const listId of ID_LIST) {
          const result = await validateValue(
            API_BASE_URL,
            listId,
            String(email).trim(),
            TIMEOUT
          );
          row[`${listId}_email`] = result ? 1 : 0;
          await sleep(REQUEST_DELAY);
        }
      }

      // Process phone validation for each list
      if (phone && String(phone).trim()) {
        for (const listId of ID_LIST) {
          const result = await validateValue(
            API_BASE_URL,
            listId,
            String(phone).trim(),
            TIMEOUT
          );
          row[`${listId}_phone`] = result ? 1 : 0;
          await sleep(REQUEST_DELAY);
        }
      }
    }

    // Save results to new Excel file
    console.log(`Saving results to: ${OUTPUT_FILE}`);

    const newWorkbook = XLSX.utils.book_new();
    const newWorksheet = XLSX.utils.json_to_sheet(data);
    XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, "Results");

    // Auto-adjust column widths
    const columnWidths = [];
    const headers = Object.keys(data[0] || {});
    headers.forEach((header, index) => {
      let maxLength = header.length;
      data.forEach((row) => {
        const cellValue = String(row[header] || "");
        if (cellValue.length > maxLength) {
          maxLength = cellValue.length;
        }
      });
      columnWidths[index] = { wch: Math.min(maxLength + 2, 50) };
    });
    newWorksheet["!cols"] = columnWidths;

    XLSX.writeFile(newWorkbook, OUTPUT_FILE);

    console.log("Process completed successfully!");
    console.log(`Results saved to: ${OUTPUT_FILE}`);

    // Display summary
    console.log("\nSummary:");
    ID_LIST.forEach((listId) => {
      const emailMatches = data.reduce(
        (sum, row) => sum + (row[`${listId}_email`] || 0),
        0
      );
      const phoneMatches = data.reduce(
        (sum, row) => sum + (row[`${listId}_phone`] || 0),
        0
      );
      console.log(
        `  ${listId}: ${emailMatches} email matches, ${phoneMatches} phone matches`
      );
    });
  } catch (error) {
    if (error.code === "ENOENT") {
      console.log(`Error: Input file not found at ${INPUT_FILE}`);
      console.log("Please check the file path and ensure the file exists.");
    } else {
      console.log(`Error: ${error.message}`);
    }
  }
}

// Export functions for testing
module.exports = {
  main,
  validateValue,
};

// Run the main function if this file is executed directly
if (require.main === module) {
  main().catch(console.error);
}
