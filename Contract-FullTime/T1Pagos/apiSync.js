const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
require("dotenv").config();

const apiToken = process.env.api_token;
const hiddenUrl = process.env.APISync_URL;

// File paths
const inputCsvFile = path.join(
  process.env.HOME || process.env.USERPROFILE,
  "Downloads",
  "Sync-RyP-20250727.csv"
);
const outputCsvFile = path.join(
  process.env.HOME || process.env.USERPROFILE,
  "Downloads",
  "SyncResponses-RyP-20250727.csv"
);

// Headers for API requests
const headers = {
  Authorization: `Bearer ${apiToken}`,
  "Content-Type": "application/json",
};

async function processApiSync() {
  try {
    // Read CSV file
    const rows = [];
    await new Promise((resolve, reject) => {
      fs.createReadStream(inputCsvFile)
        .pipe(csv())
        .on("data", (row) => rows.push(row))
        .on("end", resolve)
        .on("error", reject);
    });

    const totalIds = rows.length;
    const allData = [];

    console.log(`Processing ${totalIds} IDs from CSV file...`);

    // Process each row
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      const idValue = row["uuid"];
      const url = `${hiddenUrl}${idValue}`;

      console.log(`[${index + 1}/${totalIds}] Requesting URL: ${url}`);

      try {
        const response = await axios.patch(url, {}, { headers });

        if (response.status === 200) {
          const responseData = response.data;
          responseData.id = idValue; // Add ID to response for better understanding
          responseData.error = ""; // Empty column if no error is returned
          allData.push(responseData);
        } else {
          const errorMessage = response.data?.error || response.statusText;
          console.log(
            `Error on ID ${idValue}: ${response.status} - ${errorMessage}`
          );
          allData.push({ id: idValue, error: errorMessage });
        }
      } catch (error) {
        let errorMessage;
        if (error.response) {
          // Server responded with error status
          errorMessage =
            error.response.data?.error || error.response.statusText;
          console.log(
            `Error on ID ${idValue}: ${error.response.status} - ${errorMessage}`
          );
        } else if (error.request) {
          // Request was made but no response received
          errorMessage = "No response received";
          console.log(`Connection error on ID ${idValue}: ${errorMessage}`);
        } else {
          // Something else happened
          errorMessage = error.message;
          console.log(`Connection error on ID ${idValue}: ${errorMessage}`);
        }
        allData.push({ id: idValue, error: errorMessage });
      }
    }

    // Save all responses to CSV file
    if (allData.length > 0) {
      // Get all unique headers from the data
      const allHeaders = new Set();
      allData.forEach((item) => {
        Object.keys(item).forEach((key) => allHeaders.add(key));
      });

      const headers = Array.from(allHeaders);

      // Create CSV writer
      const csvWriter = createCsvWriter({
        path: outputCsvFile,
        header: headers.map((h) => ({ id: h, title: h })),
      });

      await csvWriter.writeRecords(allData);
      console.log(`Data successfully written to ${outputCsvFile}`);
    } else {
      console.log("No data fetched to write to CSV file.");
    }
  } catch (error) {
    console.error("Error processing API sync:", error.message);
  }
}

// Run the script
processApiSync();
