import React, { useState } from "react";
import axios from "axios";
import "./App.css";

function App() {
    const [source, setSource] = useState("ClickHouse");
    const [connection, setConnection] = useState({
        host: "localhost",
        port: "8123",
        database: "default",
        user: "default",
        jwtToken: "",
    });
    const [file, setFile] = useState(null);
    const [tables, setTables] = useState([]);
    const [selectedTable, setSelectedTable] = useState("");
    const [columns, setColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [targetTable, setTargetTable] = useState("");
    const [filePath, setFilePath] = useState("");
    const [status, setStatus] = useState("");
    const [ingestStatus, setIngestStatus] = useState("");

    const handleConnect = async () => {
        setStatus("Connecting...");
        try {
            console.log("Sending connection request with:", {
                source,
                ...connection,
            });
            const response = await axios.post("http://localhost:8000/connect", {
                source,
                ...connection,
            });
            if (response.data.success) {
                setTables(response.data.tables || []);
                setStatus("Connected");
            } else {
                setStatus(`Error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Connection error:", error);
            console.error(
                "Error details:",
                error.response?.data || "No response data"
            );
            setStatus(
                `Connection failed: ${
                    error.response?.data?.error || error.message
                }`
            );
        }
    };

    const handleLoadColumns = async () => {
        setStatus("Fetching columns...");
        const formData = new FormData();
        formData.append("source", source);
        formData.append("table", selectedTable);
        if (source === "ClickHouse") {
            formData.append("host", connection.host);
            formData.append("port", connection.port);
            formData.append("database", connection.database);
            formData.append("user", connection.user);
            formData.append("jwtToken", connection.jwtToken);
        } else if (file) {
            formData.append("file", file);
        }
        try {
            console.log("Sending column request with:", {
                source,
                table: selectedTable,
                ...connection,
            });
            const response = await axios.post(
                "http://localhost:8000/columns",
                formData
            );
            if (response.data.success) {
                console.log("Received columns:", response.data.columns);
                setColumns(response.data.columns);
                if (response.data.filePath) {
                    setFilePath(response.data.filePath);
                }
                setStatus("Columns loaded");
            } else {
                setStatus(`Error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Error loading columns:", error);
            console.error(
                "Error details:",
                error.response?.data || "No response data"
            );
            setStatus(
                `Failed to load columns: ${
                    error.response?.data?.error || error.message
                }`
            );
        }
    };

    const toggleColumn = (column) => {
        setSelectedColumns((prev) =>
            prev.includes(column.name)
                ? prev.filter((c) => c !== column.name)
                : [...prev, column.name]
        );
    };

    const handleIngestData = async () => {
        if (selectedColumns.length === 0) {
            setIngestStatus("Please select at least one column for ingestion");
            return;
        }

        if (!targetTable) {
            setIngestStatus("Please enter a target table name");
            return;
        }

        setIngestStatus("Ingesting data...");

        try {
            const formData = new FormData();
            formData.append("source", source);

            if (source === "ClickHouse") {
                formData.append("table", selectedTable);
            } else if (source === "Flat File") {
                if (file) {
                    formData.append("file", file);
                } else if (filePath) {
                    formData.append("filePath", filePath);
                }
            }

            formData.append("columns", JSON.stringify(selectedColumns));
            formData.append("targetTable", targetTable);
            formData.append("host", connection.host);
            formData.append("port", connection.port);
            formData.append("database", connection.database);
            formData.append("user", connection.user);
            formData.append("jwtToken", connection.jwtToken);

            const response = await axios.post(
                "http://localhost:8000/ingest",
                formData
            );

            if (response.data.success) {
                setIngestStatus(`Success: ${response.data.message}`);
            } else {
                setIngestStatus(`Error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Error during ingestion:", error);
            setIngestStatus(
                `Ingestion failed: ${
                    error.response?.data?.error || error.message
                }`
            );
        }
    };

    return (
        <div className="h-screen relative mx-auto">
            <hr className="absolute border w-full top-10" />
            <hr className="absolute border left-10 h-full" />
            <div className="px-14 py-12">
                <h1 className="text-4xl font-bold">Data Ingestion Tool</h1>
                <div className="py-4">
                    <label>Select Source: </label>
                    <select
                        value={source}
                        onChange={(e) => setSource(e.target.value)}
                    >
                        <option value="ClickHouse">ClickHouse</option>
                        <option value="Flat File">Flat File</option>
                    </select>
                </div>

                <div>
                    {source === "ClickHouse" ? (
                        <div className="flex gap-2">
                            <label>Host: </label>
                            <input
                                value={connection.host}
                                onChange={(e) =>
                                    setConnection({
                                        ...connection,
                                        host: e.target.value,
                                    })
                                }
                                className="border"
                            />
                            <label>Port: </label>
                            <input
                                value={connection.port}
                                onChange={(e) =>
                                    setConnection({
                                        ...connection,
                                        port: e.target.value,
                                    })
                                }
                                className="border"
                            />
                            <label>Database: </label>
                            <input
                                value={connection.database}
                                onChange={(e) =>
                                    setConnection({
                                        ...connection,
                                        database: e.target.value,
                                    })
                                }
                                className="border"
                            />
                            <label>User: </label>
                            <input
                                value={connection.user}
                                onChange={(e) =>
                                    setConnection({
                                        ...connection,
                                        user: e.target.value,
                                    })
                                }
                                className="border"
                            />
                            <label>Password </label>
                            <input
                                type="password"
                                value={connection.jwtToken}
                                onChange={(e) =>
                                    setConnection({
                                        ...connection,
                                        jwtToken: e.target.value,
                                    })
                                }
                                className="border"
                            />
                        </div>
                    ) : (
                        <div>
                            <label>Upload CSV: </label>
                            <input
                                type="file"
                                accept=".csv"
                                onChange={(e) => setFile(e.target.files[0])}
                            />
                        </div>
                    )}
                </div>
                <button onClick={handleConnect} className="border">
                    Connect
                </button>
                {tables.length > 0 && source === "ClickHouse" && (
                    <div>
                        <label>Select Table: </label>
                        <select
                            value={selectedTable}
                            onChange={(e) => setSelectedTable(e.target.value)}
                        >
                            <option value="">Choose a table</option>
                            {tables.map((table) => (
                                <option key={table} value={table}>
                                    {table}
                                </option>
                            ))}
                        </select>
                    </div>
                )}
                {(selectedTable || source === "Flat File") && (
                    <button onClick={handleLoadColumns}>Load Columns</button>
                )}
                {columns.length > 0 && (
                    <div>
                        <h3>Select Columns:</h3>
                        {columns.map((column) => (
                            <div key={column.name}>
                                <input
                                    type="checkbox"
                                    checked={selectedColumns.includes(
                                        column.name
                                    )}
                                    onChange={() => toggleColumn(column)}
                                />
                                <label>
                                    {column.name} ({column.type})
                                </label>
                            </div>
                        ))}

                        {selectedColumns.length > 0 && (
                            <div className="ingest-section">
                                <h3>Data Ingestion</h3>
                                <div>
                                    <label>Target Table Name: </label>
                                    <input
                                        value={targetTable}
                                        onChange={(e) =>
                                            setTargetTable(e.target.value)
                                        }
                                        placeholder="Enter target table name"
                                    />
                                </div>
                                <button
                                    onClick={handleIngestData}
                                    disabled={
                                        !targetTable ||
                                        selectedColumns.length === 0
                                    }
                                >
                                    Ingest Data
                                </button>
                                {ingestStatus && (
                                    <p className="ingest-status">
                                        {ingestStatus}
                                    </p>
                                )}
                            </div>
                        )}
                    </div>
                )}
                <p>Status: {status}</p>
            </div>
        </div>
    );
}

export default App;
