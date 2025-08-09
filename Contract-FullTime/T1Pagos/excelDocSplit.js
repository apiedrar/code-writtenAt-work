const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

/**
 * Split Excel file into batches of specified rows each
 * @param {string} inputFilePath - Path to the input .xlsx file
 * @param {string} outputDir - Directory to save output files (optional, defaults to current directory)
 * @param {number} batchSize - Number of rows per batch (default: 2000)
 * @param {string} outputPrefix - Prefix for output files (default: "Filename")
 */
function splitExcelIntoBatches(
  inputFilePath,
  outputDir = "./",
  batchSize = 2000,
  outputPrefix = "Filename"
) {
  try {
    // Read the input Excel file
    const workbook = XLSX.readFile(inputFilePath);
    const sheetName = workbook.SheetNames[0]; // Get first sheet
    const worksheet = workbook.Sheets[sheetName];

    console.log(`Reading sheet: ${sheetName}`);

    // Convert worksheet to JSON to work with data
    const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

    // Extract headers (first row)
    const headers = jsonData[0];
    console.log("Headers found:", headers);

    // Extract data rows (excluding header)
    const dataRows = jsonData.slice(1);
    console.log(`Total data rows: ${dataRows.length}`);

    // Validate that we have the expected columns (if they exist)
    if (headers && headers.length > 0) {
      if (
        !headers.includes("Primary_Key") ||
        !headers.includes("Foreign_Key")
      ) {
        console.warn(
          'Info: Expected columns "Primary_Key" and "Foreign_Key" not found in headers'
        );
        console.log("Available columns:", headers);
      }
    }

    // Create output directory if it doesn't exist
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    let batchNumber = 1; // Start counting from 1
    const totalBatches = Math.ceil(dataRows.length / batchSize);

    console.log(`Splitting into batches of ${batchSize} rows each...`);
    console.log(`Expected number of batches: ${totalBatches}`);

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
      const outputFileName = `${outputPrefix}-Batch${batchNumber}.xlsx`;
      const outputPath = path.join(outputDir, outputFileName);

      // Write the file
      XLSX.writeFile(newWorkbook, outputPath);

      console.log(
        `Created: ${outputFileName} with ${batchData.length} data rows (${
          i + 1
        }-${Math.min(i + batchSize, dataRows.length)} of ${dataRows.length})`
      );

      batchNumber++;
    }

    console.log(
      `\nSplitting complete! Created ${
        batchNumber - 1
      } batch files in: ${outputDir}`
    );
    return {
      totalRows: dataRows.length,
      batchesCreated: batchNumber - 1,
      batchSize: batchSize,
      outputDirectory: outputDir,
    };
  } catch (error) {
    console.error("Error processing Excel file:", error.message);
    throw error;
  }
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
    batchSize: 2000,
    prefix: "Filename",
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
      case "-b":
      case "--batch-size":
        if (nextArg && !nextArg.startsWith("-")) {
          const size = parseInt(nextArg);
          if (size > 0) {
            parsedArgs.batchSize = size;
          } else {
            console.warn(`Invalid batch size: ${nextArg}. Using default: 2000`);
          }
          i++; // Skip next argument as it's the value
        }
        break;
      case "-p":
      case "--prefix":
        if (nextArg && !nextArg.startsWith("-")) {
          parsedArgs.prefix = nextArg;
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
Usage: node excelDocSplit.js [options]

Options:
  -i, --input <path>        Path to input Excel file (.xlsx)
  -o, --output <path>       Output directory for batch files
  -b, --batch-size <n>      Number of rows per batch (default: 2000)
  -p, --prefix <name>       Prefix for output files (default: "Filename")
  -h, --help                Display this help message

Examples:
  # Use default settings
  node excelDocSplit.js

  # Specify input and output
  node excelDocSplit.js -i data.xlsx -o ./batches/

  # Custom batch size and prefix
  node excelDocSplit.js -i large_file.xlsx -b 5000 -p "DataBatch"

  # Combine multiple options
  node excelDocSplit.js -i input.xlsx -o ./output/ -b 1000 -p "Split"

Default behavior (no arguments):
  - Input: ~/Documents/inputExcelDocument.xlsx
  - Output: ~/Documents/outputExcelDocument/ (directory)
  - Batch size: 2000 rows
  - Prefix: "Filename"

Output files will be named: {prefix}-Batch1.xlsx, {prefix}-Batch2.xlsx, etc.
`);
}

/**
 * Generate default file paths
 * @returns {Object} Object with inputFile and outputDir paths
 */
function generateDefaultPaths() {
  const homeDir = require("os").homedir();
  const inputFile = path.join(homeDir, "Documents", "inputExcelDocument.xlsx");
  const outputDir = path.join(homeDir, "Documents", "outputExcelDocument");

  return { inputFile, outputDir };
}

/**
 * Validate file paths and arguments
 * @param {Object} args - Parsed arguments
 * @returns {Object} Validated file paths
 */
function validateAndPreparePaths(args) {
  let inputFile, outputDir;

  // Handle input file
  if (args.input) {
    inputFile = path.resolve(args.input);
    if (!fs.existsSync(inputFile)) {
      throw new Error(`Input file does not exist: ${inputFile}`);
    }
    if (!inputFile.toLowerCase().endsWith(".xlsx")) {
      console.warn(
        `Warning: Input file doesn't have .xlsx extension: ${inputFile}`
      );
    }
  } else {
    // Use default path
    const defaultPaths = generateDefaultPaths();
    inputFile = defaultPaths.inputFile;
    if (!fs.existsSync(inputFile)) {
      throw new Error(
        `Default input file does not exist: ${inputFile}. Please specify input file with -i option.`
      );
    }
  }

  // Handle output directory
  if (args.output) {
    outputDir = path.resolve(args.output);
  } else {
    // Use default path
    const defaultPaths = generateDefaultPaths();
    outputDir = defaultPaths.outputDir;
  }

  // Ensure output directory exists
  if (!fs.existsSync(outputDir)) {
    console.log(`Creating output directory: ${outputDir}`);
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // Validate batch size
  if (args.batchSize <= 0) {
    throw new Error(
      `Invalid batch size: ${args.batchSize}. Must be greater than 0.`
    );
  }

  return { inputFile, outputDir };
}

/**
 * Main execution function
 */
async function main() {
  try {
    // Parse command line arguments
    const args = parseArguments();

    // Display help if requested
    if (args.help) {
      displayHelp();
      return;
    }

    console.log("Starting Excel file splitting process...");
    console.log(`Batch size: ${args.batchSize} rows`);
    console.log(`Output prefix: ${args.prefix}`);

    // Validate and prepare file paths
    const { inputFile, outputDir } = validateAndPreparePaths(args);

    console.log(`Input file: ${inputFile}`);
    console.log(`Output directory: ${outputDir}`);

    // Check if input file is actually readable
    try {
      const stats = fs.statSync(inputFile);
      console.log(
        `Input file size: ${(stats.size / (1024 * 1024)).toFixed(2)} MB`
      );
    } catch (error) {
      throw new Error(`Cannot access input file: ${error.message}`);
    }

    // Process the file
    const result = splitExcelIntoBatches(
      inputFile,
      outputDir,
      args.batchSize,
      args.prefix
    );

    console.log("\n📊 Summary:");
    console.log(
      `✅ Total rows processed: ${result.totalRows.toLocaleString()}`
    );
    console.log(`📁 Batch files created: ${result.batchesCreated}`);
    console.log(`📋 Rows per batch: ${result.batchSize.toLocaleString()}`);
    console.log(`📂 Output location: ${result.outputDirectory}`);

    console.log("\n🎉 Process completed successfully!");
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
    console.log("\nTry running with --help for usage information.");
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

// Export the function for use in other modules
module.exports = {
  splitExcelIntoBatches,
  parseArguments,
  generateDefaultPaths,
};
