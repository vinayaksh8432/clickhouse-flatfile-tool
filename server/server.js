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
        const columns = [];
        fs.createReadStream(req.file.path)
            .pipe(parse({ delimiter: ",", columns: true }))
            .on("data", (row) => {
                if (!columns.length) {
                    columns.push(...Object.keys(row));
                }
            })
            .on("end", () => {
                console.log("Successfully parsed columns:", columns);
                // Keep the file for later use in ingestion
                res.json({
                    success: true,
                    columns: columns.map((name) => ({ name, type: "String" })),
                    filePath: req.file.path,
                });
            })
            .on("error", (error) => {
                console.error("Error parsing uploaded file:", error);
                try {
                    fs.unlinkSync(req.file.path);
                } catch (unlinkError) {
                    console.error("Error deleting file:", unlinkError);
                }
                res.status(400).json({
                    success: false,
                    error: error.message,
                    stack: error.stack,
                });
            });
    } else {
        console.error("No file uploaded for parsing columns");
        res.status(400).json({ success: false, error: "No file uploaded" });
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
            // Create columns definition for CREATE TABLE
            const columnsDefinition = JSON.parse(columns)
                .map((col) => `${col} String`)
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
            const columnsCSV = JSON.parse(columns).join(", ");

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
            fs.createReadStream(csvFile)
                .pipe(parse({ delimiter: ",", columns: true }))
                .on("data", (row) => {
                    // Only keep the selected columns
                    const filteredRow = {};
                    JSON.parse(columns).forEach((col) => {
                        filteredRow[col] = row[col];
                    });
                    rows.push(filteredRow);
                })
                .on("end", async () => {
                    // Insert data in batches
                    const batchSize = 1000;
                    const selectedColumns = JSON.parse(columns);

                    for (let i = 0; i < rows.length; i += batchSize) {
                        const batch = rows.slice(i, i + batchSize);

                        // Format the data for insertion
                        const values = batch
                            .map(
                                (row) =>
                                    `(${selectedColumns
                                        .map(
                                            (col) =>
                                                `'${
                                                    row[col]?.replace(
                                                        /'/g,
                                                        "''"
                                                    ) || ""
                                                }'`
                                        )
                                        .join(", ")})`
                            )
                            .join(", ");

                        if (values.length > 0) {
                            const insertQuery = `
                                INSERT INTO ${targetTable} (${selectedColumns.join(
                                ", "
                            )})
                                VALUES ${values}
                            `;

                            await client.query({
                                query: insertQuery,
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
