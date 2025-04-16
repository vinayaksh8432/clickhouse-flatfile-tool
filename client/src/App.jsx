import React, { useState, useEffect } from "react";
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
    const [showDownloadButton, setShowDownloadButton] = useState(false);
    const [previewData, setPreviewData] = useState([]);
    const [previewColumns, setPreviewColumns] = useState([]);
    const [showPreview, setShowPreview] = useState(false);
    const [isConnected, setIsConnected] = useState(false);
    const [isLoadingColumns, setIsLoadingColumns] = useState(false);

    // Clear selected columns when source or table changes
    useEffect(() => {
        setColumns([]);
        setSelectedColumns([]);
        setTargetTable("");
        setShowDownloadButton(false);
    }, [source, selectedTable]);

    // Clear file when switching sources
    useEffect(() => {
        if (source !== "Flat File") {
            setFile(null);
        }
    }, [source]);

    const handleConnect = async () => {
        setStatus("Connecting...");
        setIsLoadingColumns(true);
        try {
            const response = await axios.post("http://localhost:8000/connect", {
                source,
                ...connection,
            });
            if (response.data.success) {
                setTables(response.data.tables || []);
                setStatus("Connected");
                setIsConnected(true);
            } else {
                setStatus(`Error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Connection error:", error);
            setStatus(
                `Connection failed: ${
                    error.response?.data?.error || error.message
                }`
            );
        } finally {
            setIsLoadingColumns(false);
        }
    };

    const handleLoadColumns = async () => {
        setStatus("Fetching columns...");
        setIsLoadingColumns(true);
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
            const response = await axios.post(
                "http://localhost:8000/columns",
                formData
            );
            if (response.data.success) {
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
            setStatus(
                `Failed to load columns: ${
                    error.response?.data?.error || error.message
                }`
            );
        } finally {
            setIsLoadingColumns(false);
        }
    };

    const toggleColumn = (column) => {
        setSelectedColumns((prev) =>
            prev.includes(column.name)
                ? prev.filter((c) => c !== column.name)
                : [...prev, column.name]
        );
    };

    const handleSelectAll = () => {
        if (selectedColumns.length === columns.length) {
            setSelectedColumns([]);
        } else {
            setSelectedColumns(columns.map((column) => column.name));
        }
    };

    const handleIngestData = async () => {
        if (selectedColumns.length === 0) {
            setStatus("Please select at least one column for ingestion");
            return;
        }

        if (!targetTable) {
            setStatus("Please enter a target table name");
            return;
        }

        setStatus("Ingesting data...");
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
                setStatus(`Success: ${response.data.message}`);
                setShowDownloadButton(true);
            } else {
                setStatus(`Error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Error during ingestion:", error);
            setStatus(
                `Ingestion failed: ${
                    error.response?.data?.error || error.message
                }`
            );
        }
    };

    const handleDownloadCSV = async () => {
        try {
            setStatus("Downloading data...");
            const response = await axios({
                method: "post",
                url: "http://localhost:8000/download",
                data: {
                    tableName: targetTable,
                    host: connection.host,
                    port: connection.port,
                    database: connection.database,
                    user: connection.user,
                    jwtToken: connection.jwtToken,
                },
                responseType: "blob",
            });

            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement("a");
            link.href = url;
            link.setAttribute("download", `${targetTable}.csv`);
            document.body.appendChild(link);
            link.click();

            window.URL.revokeObjectURL(url);
            document.body.removeChild(link);
            setStatus("Download complete");
        } catch (error) {
            console.error("Error downloading data:", error);
            setStatus(`Download failed: ${error.message}`);
        }
    };

    const handlePreviewData = async () => {
        try {
            setStatus("Fetching preview data...");
            const response = await axios({
                method: "post",
                url: "http://localhost:8000/preview",
                data: {
                    tableName: targetTable,
                    host: connection.host,
                    port: connection.port,
                    database: connection.database,
                    user: connection.user,
                    jwtToken: connection.jwtToken,
                    limit: 10,
                },
            });

            if (response.data.success) {
                setPreviewColumns(response.data.columns);
                setPreviewData(response.data.data);
                setShowPreview(true);
                setStatus("Data preview loaded");
            } else {
                setStatus(`Preview error: ${response.data.error}`);
            }
        } catch (error) {
            console.error("Error previewing data:", error);
            setStatus(
                `Preview failed: ${
                    error.response?.data?.error || error.message
                }`
            );
        }
    };

    return (
        <div className="app-container">
            <header>
                <h1>ClickHouse-FlatFile Ingestion Tool</h1>
            </header>

            <main>
                <section className="data-source-section">
                    <h2>Data Source</h2>

                    <div className="source-selector">
                        <label>Source Type:</label>
                        <select
                            value={source}
                            onChange={(e) => setSource(e.target.value)}
                        >
                            <option value="ClickHouse">ClickHouse</option>
                            <option value="Flat File">Flat File</option>
                        </select>
                    </div>

                    {source === "ClickHouse" ? (
                        <div className="connection-form">
                            <div className="form-row">
                                <div className="form-field">
                                    <label>Host:</label>
                                    <input
                                        value={connection.host}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                host: e.target.value,
                                            })
                                        }
                                    />
                                </div>
                                <div className="form-field">
                                    <label>Port:</label>
                                    <input
                                        value={connection.port}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                port: e.target.value,
                                            })
                                        }
                                    />
                                </div>
                            </div>
                            <div className="form-row">
                                <div className="form-field">
                                    <label>Database:</label>
                                    <input
                                        value={connection.database}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                database: e.target.value,
                                            })
                                        }
                                    />
                                </div>
                                <div className="form-field">
                                    <label>User:</label>
                                    <input
                                        value={connection.user}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                user: e.target.value,
                                            })
                                        }
                                    />
                                </div>
                            </div>
                            <div className="form-row">
                                <div className="form-field">
                                    <label>Password:</label>
                                    <input
                                        type="password"
                                        value={connection.jwtToken}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                jwtToken: e.target.value,
                                            })
                                        }
                                    />
                                </div>
                            </div>
                            <button
                                className="action-button"
                                onClick={handleConnect}
                            >
                                Connect
                            </button>
                        </div>
                    ) : (
                        <div className="file-upload">
                            <label className="file-upload-label">
                                {file ? file.name : "Select a CSV or TXT file"}
                                <input
                                    type="file"
                                    accept=".csv,.txt"
                                    onChange={(e) => setFile(e.target.files[0])}
                                />
                            </label>
                            <button
                                className="action-button"
                                onClick={handleLoadColumns}
                                disabled={!file}
                            >
                                Upload File
                            </button>
                        </div>
                    )}

                    {/* Always show table selection for ClickHouse if connected */}
                    {source === "ClickHouse" && isConnected && (
                        <div className="table-selector">
                            <div className="form-row">
                                <div className="form-field">
                                    <label>Table:</label>
                                    <select
                                        value={selectedTable}
                                        onChange={(e) =>
                                            setSelectedTable(e.target.value)
                                        }
                                    >
                                        <option value="">Choose a table</option>
                                        {tables.map((table) => (
                                            <option key={table} value={table}>
                                                {table}
                                            </option>
                                        ))}
                                    </select>
                                </div>
                                <button
                                    className="action-button"
                                    onClick={handleLoadColumns}
                                    disabled={
                                        isLoadingColumns || !selectedTable
                                    }
                                >
                                    {isLoadingColumns
                                        ? "Loading..."
                                        : "Load Columns"}
                                </button>
                            </div>
                        </div>
                    )}
                </section>

                {columns.length > 0 && (
                    <section className="column-section">
                        <h2>Columns</h2>
                        <div className="column-header">
                            <button
                                className="select-all-button"
                                onClick={handleSelectAll}
                            >
                                {selectedColumns.length === columns.length
                                    ? "Deselect All"
                                    : "Select All"}
                            </button>
                            <div className="selected-count">
                                {selectedColumns.length} of {columns.length}{" "}
                                selected
                            </div>
                        </div>

                        <div className="column-list">
                            {columns.map((column) => (
                                <div key={column.name} className="column-item">
                                    <label className="column-label">
                                        <input
                                            type="checkbox"
                                            checked={selectedColumns.includes(
                                                column.name
                                            )}
                                            onChange={() =>
                                                toggleColumn(column)
                                            }
                                        />
                                        <span className="column-name">
                                            {column.name}
                                        </span>
                                        <span className="column-type">
                                            {column.type}
                                        </span>
                                    </label>
                                </div>
                            ))}
                        </div>

                        <div className="target-table-section">
                            <h3>Target Configuration</h3>
                            <div className="form-row">
                                <div className="form-field">
                                    <label>Target Table Name:</label>
                                    <input
                                        value={targetTable}
                                        onChange={(e) =>
                                            setTargetTable(e.target.value)
                                        }
                                        placeholder="Enter target table name"
                                    />
                                </div>
                            </div>
                            <div className="action-buttons">
                                <button
                                    className="primary-button"
                                    onClick={handleIngestData}
                                    disabled={
                                        !targetTable ||
                                        selectedColumns.length === 0
                                    }
                                >
                                    Ingest Data
                                </button>

                                {showDownloadButton && (
                                    <>
                                        <button
                                            className="secondary-button"
                                            onClick={handlePreviewData}
                                        >
                                            Preview Data
                                        </button>
                                        <button
                                            className="secondary-button"
                                            onClick={handleDownloadCSV}
                                        >
                                            Download CSV
                                        </button>
                                    </>
                                )}
                            </div>
                        </div>
                    </section>
                )}

                {status && (
                    <div className="status-message">
                        <span>{status}</span>
                    </div>
                )}
            </main>

            {showPreview && (
                <div className="modal-overlay">
                    <div className="preview-modal">
                        <div className="modal-header">
                            <h3>Data Preview: {targetTable}</h3>
                            <button
                                className="close-button"
                                onClick={() => setShowPreview(false)}
                            >
                                ×
                            </button>
                        </div>
                        <div className="modal-content">
                            {previewData.length > 0 ? (
                                <table className="preview-table">
                                    <thead>
                                        <tr>
                                            {previewColumns.map((column) => (
                                                <th key={column}>{column}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {previewData.map((row, rowIndex) => (
                                            <tr key={rowIndex}>
                                                {previewColumns.map(
                                                    (column) => (
                                                        <td
                                                            key={`${rowIndex}-${column}`}
                                                        >
                                                            {row[column]}
                                                        </td>
                                                    )
                                                )}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            ) : (
                                <div className="no-data-message">
                                    No data available for preview
                                </div>
                            )}
                        </div>
                        <div className="modal-footer">
                            <button
                                className="secondary-button"
                                onClick={() => setShowPreview(false)}
                            >
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default App;
