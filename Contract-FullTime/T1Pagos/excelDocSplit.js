const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

/**
 * Split Excel file into batches of 2000 rows each
 * @param {string} inputFilePath - Path to the input .xlsx file
 * @param {string} outputDir - Directory to save output files (optional, defaults to current directory)
 */
function splitExcelIntoBatches(inputFilePath, outputDir = "./") {
  try {
    // Read the input Excel file
    const workbook = XLSX.readFile(inputFilePath);
    const sheetName = workbook.SheetNames[0]; // Get first sheet
    const worksheet = workbook.Sheets[sheetName];

    // Convert worksheet to JSON to work with data
    const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

    // Extract headers (first row)
    const headers = jsonData[0];
    console.log("Headers found:", headers);

    // Extract data rows (excluding header)
    const dataRows = jsonData.slice(1);
    console.log(`Total data rows: ${dataRows.length}`);

    // Validate that we have the expected columns
    if (!headers.includes("Primary_Key") || !headers.includes("Foreign_Key")) {
      console.warn(
        'Warning: Expected columns "Primary_Key" and "Foreign_Key" not found in headers'
      );
      console.log("Available columns:", headers);
    }

    // Create output directory if it doesn't exist
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const batchSize = 2000;
    let batchNumber = 1; // Start counting from 1

    // Process data in batches
    for (let i = 0; i < dataRows.length; i += batchSize) {
      const batchData = dataRows.slice(i, i + batchSize);

      // Create new workbook for this batch
      const newWorkbook = XLSX.utils.book_new();

      // Combine headers with batch data
      const sheetData = [headers, ...batchData];

      // Create worksheet from data
      const newWorksheet = XLSX.utils.aoa_to_sheet(sheetData);

      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, "Sheet1");

      // Generate output filename
      const outputFileName = `Filename-Batch${batchNumber}.xlsx`;
      const outputPath = path.join(outputDir, outputFileName);

      // Write the file
      XLSX.writeFile(newWorkbook, outputPath);

      console.log(
        `Created: ${outputFileName} with ${batchData.length} data rows`
      );

      batchNumber++;
    }

    console.log(`\nSplitting complete! Created ${batchNumber} batch files.`);
  } catch (error) {
    console.error("Error processing Excel file:", error.message);
    throw error;
  }
}

// Example usage
function main() {
  // Replace with your input file path
  const inputFile = path.join(
    require("os").homedir(),
    "Documents",
    "inputExcelDocument.xlsx"
  );
  const outputDirectory = path.join(
    require("os").homedir(),
    "Documents",
    "outputExcelDocument.xlsx"
  );

  // Check if input file exists
  if (!fs.existsSync(inputFile)) {
    console.error(`Input file not found: ${inputFile}`);
    console.log(
      "Please ensure the input file exists and update the inputFile variable."
    );
    return;
  }

  console.log(`Processing file: ${inputFile}`);
  console.log(`Output directory: ${outputDirectory}`);

  try {
    splitExcelIntoBatches(inputFile, outputDirectory);
  } catch (error) {
    console.error("Failed to process file:", error.message);
  }
}

// Run the script
if (require.main === module) {
  main();
}

// Export the function for use in other modules
module.exports = { splitExcelIntoBatches };
