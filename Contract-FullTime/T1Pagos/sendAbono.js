const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
require("dotenv").config();

// Environment variables
const apiToken = process.env.api_token;
const hiddenUrl = process.env.APISendAbono_URL;

// File paths
const inputCsvFile = path.join(
  process.env.HOME || process.env.USERPROFILE,
  "Downloads",
  "SendAbono_20250708.csv"
);
const timestamp = new Date().toISOString().replace(/[-T:]/g, "").split(".")[0];
const outputCsvFile = path.join(
  process.env.HOME || process.env.USERPROFILE,
  "Downloads",
  `SendAbono_20250708_Responses_${timestamp}.csv`
);

// API configuration
const url = hiddenUrl;
const headers = {
  Authorization: `Bearer ${apiToken}`,
  "Content-Type": "application/json",
};

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
  if (key === "uuid") {
    return value; // Skip uuid as it's not part of the payload
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

// Main function
async function main() {
  try {
    // Read data from CSV file
    const rows = await readCsvFile(inputCsvFile);
    const totalRows = rows.length;

    // List storing all API responses and errors
    const allData = [];

    // Process each row in CSV file
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];

      // Create payload from row data
      const payload = {};

      // Convert types appropriately
      for (const [key, value] of Object.entries(row)) {
        if (key === "uuid") {
          continue; // Skip uuid as it's not part of the payload
        }

        payload[key] = convertValue(key, value);
      }

      // Apply fixed values, overriding any from the CSV
      Object.assign(payload, FIXED_VALUES);

      // Get the ID for the request URL
      const idValue = row.uuid || "";
      const requestUrl = url;

      console.log(`[${index + 1}/${totalRows}] Sending to URL: ${requestUrl}`);
      console.log(`Payload: ${JSON.stringify(payload, null, 2)}`);

      try {
        const response = await axios.post(requestUrl, payload, { headers });
        const responseData = response.data || {};

        if (response.status === 200) {
          responseData.id = idValue; // Add ID to response
          responseData.error = ""; // Empty column if no error is returned
          allData.push(responseData);
        } else {
          const errorMessage = responseData.error || response.statusText;
          console.log(
            `Error on ID ${idValue}: ${response.status} - ${errorMessage}`
          );
          allData.push({ id: idValue, error: errorMessage });
        }
      } catch (error) {
        let errorMessage;
        if (error.response) {
          // The request was made and the server responded with a status code
          errorMessage =
            error.response.data?.error || error.response.statusText;
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

        allData.push({ id: idValue, error: errorMessage });
      }
    }

    // Save all responses to CSV file
    await writeCsvFile(outputCsvFile, allData);
  } catch (error) {
    console.error(`Error reading CSV file: ${error}`);
  }
}

// Run the main function
main().catch(console.error);
