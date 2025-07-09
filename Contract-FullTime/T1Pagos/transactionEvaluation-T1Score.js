const fs = require("fs");
const path = require("path");
const os = require("os");
const { v4: uuidv4 } = require("uuid");
const axios = require("axios");
const XLSX = require("xlsx");
const { faker } = require("@faker-js/faker");
require("dotenv").config();

// Configuration
const originacionKey = process.env.Originacion_ClaroScore;
const hiddenUrl = process.env.originacion_api_url;
const timestamp = new Date()
  .toISOString()
  .replace(/[-:]/g, "")
  .replace("T", "_")
  .split(".")[0];

// File paths
const DATA_XLSX_PATH = path.join(
  os.homedir(),
  "Downloads",
  "Data_TransactionEvaluations-ClaroScore_Originacion-Prod.xlsx"
);
const OUTPUT_EXCEL = path.join(
  os.homedir(),
  "Downloads",
  `Responses-TransactionEvaluation-${timestamp}.xlsx`
);

const headers = {
  "Content-Type": "application/json",
  "x-api-key": originacionKey,
};

/**
 * Flatten nested object into single level with underscore notation keys
 */
function flattenDict(obj, parentKey = "", sep = "_") {
  const items = [];

  for (const [key, value] of Object.entries(obj)) {
    const newKey = parentKey ? `${parentKey}${sep}${key}` : key;

    if (value && typeof value === "object" && !Array.isArray(value)) {
      items.push(...Object.entries(flattenDict(value, newKey, sep)));
    } else if (Array.isArray(value)) {
      value.forEach((item, index) => {
        if (item && typeof item === "object") {
          items.push(
            ...Object.entries(flattenDict(item, `${newKey}_${index}`, sep))
          );
        } else {
          items.push([`${newKey}_${index}`, item]);
        }
      });
    } else {
      items.push([newKey, value]);
    }
  }

  return Object.fromEntries(items);
}

/**
 * Generate random device fingerprint following the same structure as original
 */
function generateDeviceFingerprint() {
  // Generate random alphanumeric string similar to "1q2w3e4r5t6y7u8i9o0pazsxdcfv"
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let fingerprint = "";
  for (let i = 0; i < 32; i++) {
    fingerprint += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return fingerprint;
}

/**
 * Load email, phone data, and config columns from Excel file
 */
function loadData() {
  try {
    // Check if file exists
    if (!fs.existsSync(DATA_XLSX_PATH)) {
      throw new Error(`Excel file not found at: ${DATA_XLSX_PATH}`);
    }

    const workbook = XLSX.readFile(DATA_XLSX_PATH);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);

    if (data.length === 0) {
      throw new Error("Excel file is empty");
    }

    // Get column names
    const columns = Object.keys(data[0]);

    // Check for required base columns
    const requiredBaseColumns = ["email", "number"];
    let availableColumns = columns;

    // Handle case where email might be in first column without proper header
    if (!availableColumns.includes("email") && availableColumns.length > 0) {
      // Rename first column to email if it looks like email data
      const firstColumnData = data[0][availableColumns[0]];
      if (
        typeof firstColumnData === "string" &&
        firstColumnData.includes("@")
      ) {
        data.forEach((row) => {
          row.email = row[availableColumns[0]];
          delete row[availableColumns[0]];
        });
        availableColumns = ["email", ...availableColumns.slice(1)];
      }
    }

    const missingBaseColumns = requiredBaseColumns.filter(
      (col) => !availableColumns.includes(col)
    );

    if (missingBaseColumns.length > 0) {
      throw new Error(
        `Missing required base columns: ${missingBaseColumns.join(
          ", "
        )}\nAvailable columns: ${availableColumns.join(", ")}`
      );
    }

    // Clean and validate data
    const cleanedData = data.filter((row) => row.email && row.number);

    // Convert phone numbers to strings and clean them
    cleanedData.forEach((row) => {
      row.number = String(row.number).trim();
      row.email = String(row.email).trim();
    });

    // Identify config columns
    const configColumns = availableColumns.filter(
      (col) => !requiredBaseColumns.includes(col)
    );

    console.log(`✅ Loaded ${cleanedData.length} records from Excel`);
    console.log(
      `📋 Found ${configColumns.length} config columns: ${configColumns
        .slice(0, 5)
        .join(", ")}${configColumns.length > 5 ? "..." : ""}`
    );

    // Add config columns info to each record
    cleanedData.forEach((record) => {
      record._config_columns = configColumns;
    });

    return cleanedData;
  } catch (error) {
    console.log(`❌ Error loading data: ${error.message}`);
    console.log("📋 Using fallback data...");

    // Fallback data
    return [
      { email: "test1@example.com", number: "5511111111", _config_columns: [] },
      { email: "test2@example.com", number: "3322222222", _config_columns: [] },
      { email: "test3@example.com", number: "8133333333", _config_columns: [] },
      { email: "test4@example.com", number: "5544444444", _config_columns: [] },
      { email: "test5@example.com", number: "3355555555", _config_columns: [] },
    ];
  }
}

/**
 * Generate config object from row data based on config columns and their boolean values
 */
function generateConfigFromRow(rowData) {
  const configColumns = rowData._config_columns || [];
  const config = {};

  configColumns.forEach((column) => {
    if (column in rowData) {
      let value = rowData[column];

      // Convert string representations to boolean
      if (typeof value === "string") {
        value = value.toLowerCase().trim();
        if (["true", "1", "yes", "y"].includes(value)) {
          config[column] = true;
        } else if (["false", "0", "no", "n"].includes(value)) {
          config[column] = false;
        } else {
          config[column] = false;
        }
      } else if (typeof value === "number") {
        config[column] = Boolean(value);
      } else if (typeof value === "boolean") {
        config[column] = value;
      } else {
        config[column] = false;
      }
    }
  });

  // If no config columns found, fall back to original static config
  if (Object.keys(config).length === 0) {
    const defaultConfigKeys = [
      "profile_idx",
      "tch_adaptability_idx",
      "ctbility_idx",
      "bill_address_to_full_name_confidence",
      "bill_address_to_last_name_confidence",
      "bill_city_postal_match",
      "billing_risk_country",
      "card_category",
      "card_type",
      "company_name",
      "customers_phone_in_billing_location",
      "dis_description",
      "domain_age",
      "domain_category",
      "domain_corporate",
      "domain_country_code",
      "domain_country_match",
      "domain_creation_days",
      "domain_exists",
      "domain_name",
      "domain_relevant_info",
      "domain_risk",
      "domain_risk_level",
      "ea_advice",
      "ea_reason",
      "ea_reason_id",
      "ea_risk_band_id",
      "ea_score",
      "email_age",
      "email_creation_days",
      "email_exists",
      "email_owner",
      "email_to_bill_address_confidence",
      "email_to_full_name_confidence",
      "email_to_ip_confidence",
      "email_to_last_name_confidence",
      "email_to_phone_confidence",
      "email_to_ship_address_confidence",
      "ip_proxy_type",
      "ip_reputation",
      "last_consultation",
      "phone_name_match",
      "phone_owner",
      "phone_status",
      "phone_to_bill_address_confidence",
      "phone_to_full_name_confidence",
      "phone_to_last_name_confidence",
      "phone_to_ship_address_confidence",
      "ship_city_postal_match",
      "ship_forward",
      "sm_friends",
      "source_industry",
      "status",
      "u_hits",
      "title",
      "cp",
      "rfc",
      "calle",
      "ciudad",
      "estado",
      "genero",
      "nombre",
      "colonia",
      "materno",
      "paterno",
      "actividad",
      "antigplan",
      "fecha_nac",
      "domiciliado",
      "lim_credito",
      "suscripcion",
      "match_nombre",
      "estatus_linea",
      "match_materno",
      "match_paterno",
      "nivel_recarga",
      "rango_consumo",
      "red_contactos",
      "score_wcredito",
      "tiposuscripcion",
      "antiguedad_linea",
      "cadencia_recarga",
      "nivel_cambio_sim",
      "score_fraude_gen",
      "score_riesgo_gen",
      "tendencia_consumo",
    ];

    defaultConfigKeys.forEach((key) => {
      config[key] = true;
    });
  }

  return config;
}

/**
 * Generate API payload with email, phone number, and dynamic config
 */
function generatePayload(email, number, rowData) {
  const randInt = () => Math.floor(Math.random() * 10) + 1;
  const randFloat = () => parseFloat((Math.random() * 989.99 + 10).toFixed(2));

  // Generate dynamic config from row data
  const dynamicConfig = generateConfigFromRow(rowData);

  return {
    transaction_id: uuidv4(),
    request: {
      ipv4: faker.internet.ipv4(),
      ipv6: faker.internet.ipv6(),
    },
    purchase: {
      id: uuidv4(),
      created: "2025-04-22T12:48:10-08:00",
      shipping_address: {
        street: "Avenida Juarez",
        external_number: "213",
        internal_number: "1A",
        town: "Roma Norte",
        city: "Alcaldia Gustavo A. Madero",
        state: "MX",
        country: "MX",
        zip_code: "09960",
      },
      phone: {
        number: number,
      },
      items: [
        {
          sku: "12345",
          ean_upc: "4011 200296908",
          name: "Lentes",
          quantity: randInt(),
          unit_amount: randFloat(),
        },
        {
          sku: "12345",
          ean_upc: "4011 200296909",
          name: "Petalo 24 pzas",
          quantity: randInt(),
          unit_amount: randFloat(),
        },
      ],
      total_items: randInt(),
      delivery_date: "2024-11-07T21:20:16-06:00",
      delivery_service: "UPS",
      delivery_tracking: "12346535038485",
      delivery_amount: randFloat(),
      items_amount: randFloat(),
      total_amount: randFloat(),
      device_fingerprint: generateDeviceFingerprint(),
    },
    client: {
      id: uuidv4(),
      name: "John",
      paternal_surname: "Doe",
      maternal_surname: "Name",
      email: email,
      rfc: "VECJ880326MC",
      gender: "Hombre",
      birthdate: "1999-10-23",
      phone: {
        number: number,
      },
      address: {
        street: "Avenida Juarez",
        external_number: "213",
        internal_number: "1A",
        town: "Roma Norte",
        city: "Alcaldia Gustavo A. Madero",
        state: "MX",
        country: "MX",
        zip_code: "09960",
      },
      config: dynamicConfig,
    },
    merchant: {
      custom_1: uuidv4(),
      custom_2: "ABCD123456EFGH12",
      custom_3: number,
      custom_4: "2001:db8::1",
      custom_6: uuidv4(),
      custom_15: number,
      custom_21: "null@cybersource.com",
      custom_25: "12345-6789",
      custom_31: "http://www.ejemplo.com",
    },
    payment_method: {
      type: "debit card",
      card_token: uuidv4(),
      bin: "411111",
      expiration_month: "12",
      expiration_year: "2030",
      address: {
        street: "Avenida Juárez",
        external_number: "213",
        internal_number: "1A",
        town: "Roma Norte",
        city: "N/A",
        state: "MX",
        country: "MX",
        zip_code: "09960",
      },
      phone: {
        number: number,
      },
    },
  };
}

/**
 * Main function
 */
async function main() {
  console.log(
    "🚀 Starting ClaroScore Originación API Test with Dynamic Config"
  );
  console.log("=".repeat(60));

  // Load email, phone data, and config columns from Excel file
  const dataRecords = loadData();

  if (dataRecords.length === 0) {
    console.log("❌ No data loaded. Exiting...");
    return;
  }

  const results = [];

  console.log("\n🔄 Processing API requests...");
  console.log("-".repeat(30));

  for (let i = 0; i < dataRecords.length; i++) {
    const record = dataRecords[i];
    const { email, number } = record;
    const testNumber = i + 1;

    // Generate payload with dynamic config and make API request
    const payload = generatePayload(email, number, record);

    // Show config info for first few records
    if (testNumber <= 3) {
      const configKeys = Object.keys(payload.client.config);
      const configPreview = {};
      configKeys.slice(0, 5).forEach((key) => {
        configPreview[key] = payload.client.config[key];
      });
      console.log(
        `[${testNumber
          .toString()
          .padStart(3)}] Config preview: ${JSON.stringify(configPreview)}${
          configKeys.length > 5 ? "..." : ""
        } (Total: ${configKeys.length} keys)`
      );
    }

    let rowData = {
      test_id: testNumber,
      email: email,
      number: number,
      timestamp: new Date().toISOString().replace("T", " ").substring(0, 19),
      config_keys_count: Object.keys(payload.client.config).length,
    };

    // Add config data to results for reference
    Object.entries(payload.client.config).forEach(([key, value]) => {
      rowData[`config_${key}`] = value;
    });

    try {
      const response = await axios.post(hiddenUrl, payload, { headers });
      const statusCode = response.status;

      console.log(
        `[${testNumber.toString().padStart(3)}/${
          dataRecords.length
        }] Email: ${email
          .substring(0, 25)
          .padEnd(25)} | Phone: ${number} | Status: ${statusCode}`
      );

      rowData.status_code = statusCode;

      // Parse and flatten API response
      if (statusCode === 200) {
        try {
          const responseJson = response.data;
          const flattenedResponse = flattenDict(responseJson);
          Object.assign(rowData, flattenedResponse);
          rowData.api_response_raw = JSON.stringify(response.data);
        } catch (error) {
          rowData.api_response_raw = JSON.stringify(response.data);
          rowData.parse_error = "Failed to parse JSON response";
        }
      } else {
        rowData.api_response_raw = JSON.stringify(response.data);
        rowData.error_message = `HTTP ${statusCode} error`;
      }
    } catch (error) {
      console.log(
        `[${testNumber.toString().padStart(3)}/${
          dataRecords.length
        }] Email: ${email
          .substring(0, 25)
          .padEnd(25)} | Phone: ${number} | ERROR: ${error.message}`
      );
      rowData = {
        test_id: testNumber,
        email: email,
        number: number,
        status_code: "ERROR",
        timestamp: new Date().toISOString().replace("T", " ").substring(0, 19),
        error_message: error.message,
        api_response_raw: "",
        config_keys_count: 0,
      };
    }

    results.push(rowData);
  }

  // Create workbook and save to Excel
  const ws = XLSX.utils.json_to_sheet(results);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "API_Test_Results");

  // Auto-adjust column widths
  const columnWidths = [];
  const headers = Object.keys(results[0] || {});
  headers.forEach((header, index) => {
    let maxLength = header.length;
    results.forEach((row) => {
      const cellValue = String(row[header] || "");
      if (cellValue.length > maxLength) {
        maxLength = cellValue.length;
      }
    });
    columnWidths[index] = { wch: Math.min(maxLength + 2, 50) };
  });
  ws["!cols"] = columnWidths;

  // Save to Excel
  XLSX.writeFile(wb, OUTPUT_EXCEL);

  console.log("\n" + "=".repeat(60));
  console.log("✅ Test completed successfully!");
  console.log(`📊 Processed ${dataRecords.length} records`);
  console.log(`📁 Results saved to: ${OUTPUT_EXCEL}`);
  console.log(
    `📋 Total columns in output: ${Object.keys(results[0] || {}).length}`
  );

  // Show summary statistics
  if (results.length > 0) {
    const successCount = results.filter((r) => r.status_code === 200).length;
    const errorCount = results.filter((r) => r.status_code !== 200).length;
    console.log(`✅ Successful requests: ${successCount}`);
    console.log(`❌ Failed requests: ${errorCount}`);

    // Show config summary
    const configColumns = Object.keys(results[0]).filter((key) =>
      key.startsWith("config_")
    );
    if (configColumns.length > 0) {
      console.log(`🔧 Config columns found: ${configColumns.length}`);
    }
  }
}

// Run the main function
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, generatePayload, loadData };
