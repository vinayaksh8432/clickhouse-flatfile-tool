const express = require("express");
const cors = require("cors");
const { createClient } = require("@clickhouse/client");
const multer = require("multer");
const { parse } = require("csv-parse");
const fs = require("fs");
const path = require("path");

const app = express();
const upload = multer({ dest: "uploads/" });

// Enable CORS for both localhost:3000 and localhost:5173
app.use(cors({ origin: ["http://localhost:5173"] }));
app.use(express.json());

// Add request logging middleware
app.use((req, res, next) => {
    console.log(`${req.method} ${req.url}`);
    console.log("Request headers:", req.headers);
    console.log("Request body:", req.body);
    next();
});

// Define standard column names for UK property price data
const UK_PROPERTY_COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
];

// Test endpoint
app.get("/", (req, res) => {
    res.json({ message: "ClickHouse-FlatFile Ingestion Tool Backend" });
});

// Connect to ClickHouse and list tables
app.post("/connect", async (req, res) => {
    console.log("Connect endpoint called with body:", req.body);
    const {
        source,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!req.body || !source) {
        return res.status(400).json({
            success: false,
            error:
                "Missing required parameters. Request body: " +
                JSON.stringify(req.body),
        });
    }

    if (source === "ClickHouse") {
        try {
            console.log("Connecting to ClickHouse with:", {
                host: `http://${host}:${port}`,
                username: user,
                database,
            });

            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken, // Pass JWT as password for simplicity
                database,
            });

            const result = await client.query({
                query: "SHOW TABLES",
                format: "JSONEachRow",
            });

            const tables = await result.json();
            await client.close();

            console.log(
                "Successfully connected to ClickHouse, found tables:",
                tables
            );
            res.json({ success: true, tables: tables.map((t) => t.name) });
        } catch (error) {
            console.error("ClickHouse connection error:", error);
            res.status(400).json({
                success: false,
                error: error.message,
                stack: error.stack,
            });
        }
    } else {
        res.json({ success: true, message: "Flat File connection ready" });
    }
});

// Get columns for a table or Flat File
app.post("/columns", upload.single("file"), async (req, res) => {
    console.log("Columns endpoint called with body:", req.body);
    const {
        source,
        table,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!req.body || !source) {
        return res.status(400).json({
            success: false,
            error:
                "Missing required parameters. Request body: " +
                JSON.stringify(req.body),
        });
    }

    if (source === "ClickHouse") {
        try {
            console.log("Fetching columns from ClickHouse table:", table);
            const client = createClient({
                host: `http://${host}:${port}`,
                username: user,
                password: jwtToken,
                database,
            });

            const result = await client.query({
                query: `DESCRIBE TABLE ${table}`,
                format: "JSONEachRow",
            });

            const columns = await result.json();
            await client.close();

            console.log("Successfully fetched columns:", columns);
            // Return column objects with name and type information
            res.json({
                success: true,
                columns: columns.map((c) => ({
                    name: c.name,
                    type: c.type,
                })),
            });
        } catch (error) {
            console.error("Error fetching columns from ClickHouse:", error);
            res.status(400).json({
                success: false,
                error: error.message,
                stack: error.stack,
            });
        }
    } else if (req.file) {
        console.log("Parsing uploaded file for columns:", req.file.path);

        // Check file extension
        const fileExt = path.extname(req.file.originalname).toLowerCase();

        if (fileExt === ".csv" || fileExt === ".txt") {
            // For UK property price data files, use predefined column names
            if (
                req.file.originalname.toLowerCase().includes("pp-") ||
                req.file.originalname.toLowerCase().includes("price-paid")
            ) {
                // For UK property price data, use standard column names
                console.log(
                    "Detected UK property price data file, using standard column names"
                );
                res.json({
                    success: true,
                    columns: UK_PROPERTY_COLUMNS.map((name) => ({
                        name,
                        type: "String",
                    })),
                    filePath: req.file.path,
                });
            } else {
                // For other CSV files, attempt to read the header row
                let firstLine = "";
                let hasReadFirstLine = false;

                fs.createReadStream(req.file.path)
                    .on("data", (chunk) => {
                        if (!hasReadFirstLine) {
                            // Find the first newline to get just the header
                            const newlineIndex = chunk.indexOf("\n");
                            if (newlineIndex !== -1) {
                                firstLine += chunk
                                    .slice(0, newlineIndex)
                                    .toString();
                                hasReadFirstLine = true;
                            } else {
                                firstLine += chunk.toString();
                            }
                        }
                    })
                    .on("end", () => {
                        if (firstLine) {
                            // Split the header line by comma to get column names
                            const columnNames = firstLine
                                .split(",")
                                .map((name) => name.trim());
                            console.log(
                                "Successfully parsed columns from header:",
                                columnNames
                            );
                            res.json({
                                success: true,
                                columns: columnNames.map((name) => ({
                                    name,
                                    type: "String",
                                })),
                                filePath: req.file.path,
                            });
                        } else {
                            // Fallback to simple column numbering if header can't be parsed
                            console.log(
                                "Could not parse header, using generic column names"
                            );
                            const parser = parse({ delimiter: "," });
                            let columnCount = 0;

                            parser.on("readable", function () {
                                let record;
                                while ((record = parser.read())) {
                                    columnCount = record.length;
                                    break; // Only need the first row to count columns
                                }
                            });

                            parser.on("end", function () {
                                const columns = Array.from(
                                    { length: columnCount },
                                    (_, i) => ({
                                        name: `column_${i + 1}`,
                                        type: "String",
                                    })
                                );

                                res.json({
                                    success: true,
                                    columns,
                                    filePath: req.file.path,
                                });
                            });

                            fs.createReadStream(req.file.path).pipe(parser);
                        }
                    })
                    .on("error", (error) => {
                        console.error("Error reading file header:", error);
                        res.status(400).json({
                            success: false,
                            error: error.message,
                        });
                    });
            }
        } else {
            res.status(400).json({
                success: false,
                error: "Unsupported file format. Please upload a CSV or TXT file.",
            });
        }
    } else {
        console.error("No file uploaded for parsing columns");
        res.status(400).json({ success: false, error: "No file uploaded" });
    }
});

// Download table data as CSV
app.post("/download", async (req, res) => {
    console.log("Download endpoint called with body:", req.body);
    const {
        tableName,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
    } = req.body;

    if (!tableName) {
        return res.status(400).json({
            success: false,
            error: "Missing table name for download",
        });
    }

    try {
        // Create ClickHouse client
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        // Query data from the table
        const result = await client.query({
            query: `SELECT * FROM ${tableName}`,
            format: "CSVWithNames",
        });

        const csvData = await result.text();
        await client.close();

        // Send CSV data
        res.setHeader("Content-Type", "text/csv");
        res.setHeader(
            "Content-Disposition",
            `attachment; filename="${tableName}.csv"`
        );
        res.send(csvData);
    } catch (error) {
        console.error("Error downloading data:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Preview table data (limited rows)
app.post("/preview", async (req, res) => {
    console.log("Preview endpoint called with body:", req.body);
    const {
        tableName,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
        limit = 10,
    } = req.body;

    if (!tableName) {
        return res.status(400).json({
            success: false,
            error: "Missing table name for preview",
        });
    }

    try {
        // Create ClickHouse client
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        // First get the column names
        const columnsResult = await client.query({
            query: `DESCRIBE TABLE ${tableName}`,
            format: "JSONEachRow",
        });

        const columnsData = await columnsResult.json();
        const columns = columnsData.map((col) => col.name);

        // Query limited data from the table
        const dataResult = await client.query({
            query: `SELECT * FROM ${tableName} LIMIT ${limit}`,
            format: "JSONEachRow",
        });

        const data = await dataResult.json();
        await client.close();

        res.json({
            success: true,
            columns,
            data,
        });
    } catch (error) {
        console.error("Error previewing data:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

// Ingest data endpoint
app.post("/ingest", upload.single("file"), async (req, res) => {
    console.log("Ingest endpoint called with body:", req.body);
    const {
        source,
        table,
        columns,
        targetTable,
        host = "localhost",
        port = "8123",
        database = "default",
        user = "default",
        jwtToken = "",
        filePath,
    } = req.body;

    if (
        !source ||
        (!table && source === "ClickHouse") ||
        !targetTable ||
        !columns
    ) {
        return res.status(400).json({
            success: false,
            error: "Missing required parameters for ingestion",
        });
    }

    try {
        // Create ClickHouse client
        const client = createClient({
            host: `http://${host}:${port}`,
            username: user,
            password: jwtToken,
            database,
        });

        // Check if target table exists, if not create it
        const checkTableQuery = `
            EXISTS TABLE ${targetTable}
        `;

        const checkResult = await client.query({
            query: checkTableQuery,
            format: "JSONEachRow",
        });

        const tableExists = await checkResult.json();

        if (!tableExists[0] || tableExists[0].result === 0) {
            // Parse column names and escape them for SQL
            const selectedColumns = JSON.parse(columns);

            // Create columns definition for CREATE TABLE
            const columnsDefinition = selectedColumns
                .map((col) => `\`${col}\` String`)
                .join(", ");

            const createTableQuery = `
                CREATE TABLE ${targetTable} (
                    ${columnsDefinition}
                ) ENGINE = MergeTree()
                ORDER BY tuple()
            `;

            await client.query({
                query: createTableQuery,
            });

            console.log(`Created table ${targetTable}`);
        }

        // Handle different data sources
        if (source === "ClickHouse") {
            // Get selected columns from source table
            const selectedColumns = JSON.parse(columns);
            const columnsCSV = selectedColumns
                .map((col) => `\`${col}\``)
                .join(", ");

            // Insert data from source table to target table
            const insertQuery = `
                INSERT INTO ${targetTable} (${columnsCSV})
                SELECT ${columnsCSV} FROM ${database}.${table}
            `;

            await client.query({
                query: insertQuery,
            });

            console.log(
                `Data inserted from ${database}.${table} to ${targetTable}`
            );
        } else if (source === "Flat File") {
            // Use the file path from previous step or the newly uploaded file
            const csvFile = req.file ? req.file.path : filePath;

            if (!csvFile) {
                return res.status(400).json({
                    success: false,
                    error: "No CSV file found for ingestion",
                });
            }

            // Read CSV data
            const rows = [];
            const selectedColumns = JSON.parse(columns);

            // Check if it's a UK property price file
            const isUKPropertyFile = selectedColumns.some((col) =>
                UK_PROPERTY_COLUMNS.includes(col)
            );

            const parserOptions = {
                delimiter: ",",
                columns: isUKPropertyFile ? UK_PROPERTY_COLUMNS : true,
                skip_empty_lines: true,
                trim: true,
            };

            fs.createReadStream(csvFile)
                .pipe(parse(parserOptions))
                .on("data", (row) => {
                    // Only keep the selected columns
                    const filteredRow = {};
                    selectedColumns.forEach((col) => {
                        filteredRow[col] = row[col];
                    });
                    rows.push(filteredRow);
                })
                .on("end", async () => {
                    try {
                        // Insert data in batches
                        const batchSize = 100; // Reduced batch size to avoid query length issues

                        // Use prepared inserts instead of VALUES clause
                        const escapedColumns = selectedColumns
                            .map((col) => `\`${col}\``)
                            .join(", ");

                        for (let i = 0; i < rows.length; i += batchSize) {
                            const batch = rows.slice(i, i + batchSize);

                            if (batch.length > 0) {
                                // Use the client.insert method which is more reliable than raw SQL
                                await client.insert({
                                    table: targetTable,
                                    values: batch,
                                    format: "JSONEachRow",
                                });
                            }
                        }

                        console.log(
                            `Inserted ${rows.length} rows from CSV into ${targetTable}`
                        );

                        // Clean up
                        try {
                            fs.unlinkSync(csvFile);
                        } catch (e) {
                            console.error("Error deleting file:", e);
                        }

                        await client.close();

                        res.json({
                            success: true,
                            message: `Successfully ingested ${rows.length} rows into ${targetTable}`,
                        });
                    } catch (error) {
                        console.error("Error inserting data:", error);
                        await client.close();
                        res.status(400).json({
                            success: false,
                            error: `Error inserting data: ${error.message}`,
                        });
                    }
                })
                .on("error", (error) => {
                    console.error("Error parsing CSV for ingestion:", error);
                    client.close();
                    res.status(400).json({
                        success: false,
                        error: `Error parsing CSV: ${error.message}`,
                    });
                });

            return; // Return early as the response will be sent in the stream handlers
        }

        await client.close();

        res.json({
            success: true,
            message: `Successfully ingested data into ${targetTable}`,
        });
    } catch (error) {
        console.error("Error during ingestion:", error);
        res.status(400).json({
            success: false,
            error: error.message,
        });
    }
});

const PORT = 8000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
