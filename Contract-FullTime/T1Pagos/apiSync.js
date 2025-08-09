const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
require("dotenv").config();

const apiToken = process.env.api_token;
const hiddenUrl = process.env.APISync_URL;

// Headers for API requests
const headers = {
  Authorization: `Bearer ${apiToken}`,
  "Content-Type": "application/json",
};

/**
 * Display usage information
 */
function displayUsage() {
  console.log(`
Usage: node ${path.basename(__filename)} <inputCsvFile> [outputCsvFile]

Arguments:
  inputCsvFile     Required. Path to the input CSV file containing UUIDs
  outputCsvFile    Optional. Path for the output CSV file. 
                   If not provided, will be generated automatically in the same directory as input file

Examples:
  node ${path.basename(__filename)} ./data/input.csv
  node ${path.basename(__filename)} ./data/input.csv ./output/result.csv
  node ${path.basename(__filename)} "/path/to/Sync-RyP-20250807.csv"
  node ${path.basename(
    __filename
  )} "/path/to/input.csv" "/path/to/SyncResponses-RyP.csv"
  `);
}

async function processApiSync(inputCsvFile, outputCsvFile) {
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

      // Check if uuid column exists and has value
      if (!idValue) {
        console.log(`[${index + 1}/${totalIds}] Skipping row - no UUID found`);
        allData.push({ id: "N/A", error: "No UUID found in row" });
        continue;
      }

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

      const headersList = Array.from(allHeaders);

      // Create CSV writer
      const csvWriter = createCsvWriter({
        path: outputCsvFile,
        header: headersList.map((h) => ({ id: h, title: h })),
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
      .replace(/[-:]/g, "")
      .replace(/\..+/, "")
      .replace("T", "-");
    outputCsvFile = path.join(
      inputDir,
      `SyncResponses-${inputBasename}-${timestamp}.csv`
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

  console.log(`Input CSV file: ${inputCsvFile}`);
  console.log(`Output CSV file: ${outputCsvFile}`);

  // Check for required environment variables
  if (!apiToken) {
    console.error(
      "Error: API token not found. Make sure api_token is set in your .env file"
    );
    process.exit(1);
  }

  if (!hiddenUrl) {
    console.error(
      "Error: API URL not found. Make sure APISync_URL is set in your .env file"
    );
    process.exit(1);
  }

  // Run the sync process
  await processApiSync(inputCsvFile, outputCsvFile);
}

// Execute main function if this file is run directly
if (require.main === module) {
  main().catch(console.error);
}
