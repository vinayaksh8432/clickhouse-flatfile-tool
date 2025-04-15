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
    const [status, setStatus] = useState("Not Connected");
    const [showDownloadButton, setShowDownloadButton] = useState(false);

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
            setStatus("Please select at least one column for ingestion");
            return;
        }

        if (!targetTable) {
            setStatus("Please enter a target table name");
            return;
        }

        setStatus("Ingesting data...");
        setShowDownloadButton(false);

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
            // Using Axios to download the file with proper headers
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
                responseType: "blob", // Important for file downloads
            });

            // Create a URL for the blob
            const url = window.URL.createObjectURL(new Blob([response.data]));

            // Create a temporary link to trigger download
            const link = document.createElement("a");
            link.href = url;
            link.setAttribute("download", `${targetTable}.csv`);
            document.body.appendChild(link);
            link.click();

            // Clean up
            window.URL.revokeObjectURL(url);
            document.body.removeChild(link);
        } catch (error) {
            console.error("Error downloading data:", error);
            setStatus(`Download failed: ${error.message}`);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 text-gray-800 relative px-36 py-4">
            <h1 className="text-3xl font-semibold mb-8 text-center text-blue-600">
                ClickHouse-FlatFile Ingestion Tool
            </h1>

            <div className="grid grid-cols-2 gap-4">
                {/* Source Selection */}
                <div className="flex flex-col gap-4">
                    <div className="flex items-center gap-4">
                        <label className="text-sm font-medium w-28">
                            Data Source:
                        </label>
                        <select
                            value={source}
                            onChange={(e) => setSource(e.target.value)}
                            className="px-3 py-2 border border-gray-200 rounded w-full focus:outline-none focus:border-blue-500"
                        >
                            <option value="ClickHouse">ClickHouse</option>
                            <option value="Flat File">Flat File</option>
                        </select>
                    </div>

                    {/* Connection Details */}
                    <div>
                        {source === "ClickHouse" ? (
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4 pl-28">
                                <div className="flex flex-col">
                                    <label className="text-sm text-gray-600 mb-1">
                                        Host:
                                    </label>
                                    <input
                                        value={connection.host}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                host: e.target.value,
                                            })
                                        }
                                        className="px-3 py-2 border border-gray-200 rounded focus:outline-none focus:border-blue-500"
                                    />
                                </div>
                                <div className="flex flex-col">
                                    <label className="text-sm text-gray-600 mb-1">
                                        Port:
                                    </label>
                                    <input
                                        value={connection.port}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                port: e.target.value,
                                            })
                                        }
                                        className="px-3 py-2 border border-gray-200 rounded focus:outline-none focus:border-blue-500"
                                    />
                                </div>
                                <div className="flex flex-col">
                                    <label className="text-sm text-gray-600 mb-1">
                                        Database:
                                    </label>
                                    <input
                                        value={connection.database}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                database: e.target.value,
                                            })
                                        }
                                        className="px-3 py-2 border border-gray-200 rounded focus:outline-none focus:border-blue-500"
                                    />
                                </div>
                                <div className="flex flex-col">
                                    <label className="text-sm text-gray-600 mb-1">
                                        User:
                                    </label>
                                    <input
                                        value={connection.user}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                user: e.target.value,
                                            })
                                        }
                                        className="px-3 py-2 border border-gray-200 rounded focus:outline-none focus:border-blue-500"
                                    />
                                </div>
                                <div className="flex flex-col">
                                    <label className="text-sm text-gray-600 mb-1">
                                        Password:
                                    </label>
                                    <input
                                        type="password"
                                        value={connection.jwtToken}
                                        onChange={(e) =>
                                            setConnection({
                                                ...connection,
                                                jwtToken: e.target.value,
                                            })
                                        }
                                        className="px-3 py-2 border border-gray-200 rounded focus:outline-none focus:border-blue-500"
                                    />
                                </div>
                            </div>
                        ) : (
                            <div className="flex justify-center w-full mb-4">
                                <div className="w-3/4">
                                    <label
                                        htmlFor="file-upload"
                                        className="flex flex-col items-center justify-center w-full h-32 px-4 transition bg-white border-2 border-gray-300 border-dashed rounded-md appearance-none cursor-pointer hover:border-blue-500 focus:outline-none"
                                    >
                                        <span className="flex items-center space-x-2">
                                            <svg
                                                xmlns="http://www.w3.org/2000/svg"
                                                className="w-6 h-6 text-gray-600"
                                                fill="none"
                                                viewBox="0 0 24 24"
                                                stroke="currentColor"
                                                strokeWidth="2"
                                            >
                                                <path
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                    d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                                                />
                                            </svg>
                                            <span className="font-medium text-gray-600">
                                                {file
                                                    ? file.name
                                                    : "Drop files here or click to upload"}
                                            </span>
                                        </span>
                                        <span className="text-xs text-gray-500 mt-2">
                                            Supported formats: CSV, TXT
                                        </span>
                                        <input
                                            id="file-upload"
                                            type="file"
                                            accept=".csv,.txt"
                                            onChange={(e) =>
                                                setFile(e.target.files[0])
                                            }
                                            className="hidden"
                                        />
                                    </label>
                                </div>
                            </div>
                        )}
                    </div>

                    <div className="mx-auto flex justify-center">
                        {source === "ClickHouse" ? (
                            <button
                                onClick={handleConnect}
                                className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
                            >
                                Connect
                            </button>
                        ) : (
                            <button
                                onClick={handleLoadColumns}
                                disabled={!file}
                                className={`px-4 py-2 rounded text-white transition-colors ${
                                    !file
                                        ? "bg-gray-300 cursor-not-allowed"
                                        : "bg-blue-500 hover:bg-blue-600"
                                }`}
                            >
                                Choose File
                            </button>
                        )}
                    </div>
                    {/* Table Selection */}
                    <div>
                        {source === "ClickHouse" &&
                            (status === "Connected" ||
                                status === "Columns loaded" ||
                                status.includes("Success")) && (
                                <div className="flex gap-2">
                                    <div className="flex items-center gap-4 w-full">
                                        <label className="text-sm font-medium text-nowrap">
                                            Select Table:
                                        </label>
                                        <select
                                            value={selectedTable}
                                            onChange={(e) =>
                                                setSelectedTable(e.target.value)
                                            }
                                            className="px-3 py-2 border border-gray-200 rounded w-full focus:outline-none focus:border-blue-500"
                                        >
                                            <option value="">
                                                Choose a table
                                            </option>
                                            {tables.map((table) => (
                                                <option
                                                    key={table}
                                                    value={table}
                                                >
                                                    {table}
                                                </option>
                                            ))}
                                        </select>
                                    </div>
                                    <button
                                        onClick={handleLoadColumns}
                                        className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors text-nowrap"
                                    >
                                        Load Columns
                                    </button>
                                </div>
                            )}
                    </div>

                    <div>
                        {source === "ClickHouse" &&
                            !(
                                status === "Connected" ||
                                status === "Columns loaded" ||
                                status.includes("Success")
                            ) && (
                                <div className="opacity-50">
                                    <div className="flex items-center gap-4 mb-2">
                                        <label className="text-sm font-medium w-28">
                                            Select Table:
                                        </label>
                                        <select
                                            disabled
                                            className="px-3 py-2 border border-gray-200 rounded w-full focus:outline-none focus:border-blue-500 cursor-not-allowed"
                                        >
                                            <option value="">
                                                Connect first to see tables
                                            </option>
                                        </select>
                                    </div>
                                    <div className="pl-28">
                                        <button
                                            disabled
                                            className="px-4 py-2 bg-gray-300 text-gray-500 rounded cursor-not-allowed"
                                        >
                                            Load Columns
                                        </button>
                                    </div>
                                </div>
                            )}
                    </div>
                </div>

                <div className="">
                    {/* Column Selection */}
                    {columns.length > 0 && (
                        <div className="mb-6">
                            <h3 className="text-lg font-medium mb-3">
                                Select Columns:
                            </h3>
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2 items-center mb-6">
                                {columns.map((column) => (
                                    <div
                                        key={column.name}
                                        className="flex items-center"
                                    >
                                        <input
                                            type="checkbox"
                                            id={column.name}
                                            checked={selectedColumns.includes(
                                                column.name
                                            )}
                                            onChange={() =>
                                                toggleColumn(column)
                                            }
                                            className="mr-2"
                                        />
                                        <label
                                            htmlFor={column.name}
                                            className="text-sm"
                                        >
                                            {column.name}{" "}
                                            <span className="text-xs text-gray-500">
                                                ({column.type})
                                            </span>
                                        </label>
                                    </div>
                                ))}
                            </div>

                            {/* Data Ingestion */}
                            {selectedColumns.length > 0 && (
                                <div className="p-4 bg-gray-100 rounded">
                                    <div className="flex justify-between items-center mb-3">
                                        <h3 className="text-lg font-medium">
                                            Target Table
                                        </h3>
                                        {showDownloadButton && (
                                            <button
                                                onClick={handleDownloadCSV}
                                                className="text-blue-500 hover:text-blue-700 focus:outline-none"
                                                title="Download CSV"
                                            >
                                                <svg
                                                    xmlns="http://www.w3.org/2000/svg"
                                                    className="h-6 w-6"
                                                    fill="none"
                                                    viewBox="0 0 24 24"
                                                    stroke="currentColor"
                                                >
                                                    <path
                                                        strokeLinecap="round"
                                                        strokeLinejoin="round"
                                                        strokeWidth={2}
                                                        d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
                                                    />
                                                </svg>
                                            </button>
                                        )}
                                    </div>
                                    <div className="flex gap-4 items-center mb-4">
                                        <label className="text-sm font-medium w-28">
                                            Table Name:
                                        </label>
                                        <input
                                            value={targetTable}
                                            onChange={(e) =>
                                                setTargetTable(e.target.value)
                                            }
                                            placeholder="Enter target table name"
                                            className="px-3 py-2 border border-gray-200 rounded w-full focus:outline-none focus:border-blue-500"
                                        />
                                    </div>
                                    <div className="flex justify-between items-center">
                                        <div className="text-sm">
                                            <span className="text-blue-600 font-medium">
                                                {selectedColumns.length}
                                            </span>{" "}
                                            columns selected
                                        </div>
                                        <button
                                            onClick={handleIngestData}
                                            disabled={
                                                !targetTable ||
                                                selectedColumns.length === 0
                                            }
                                            className={`px-4 py-2 rounded transition-colors ${
                                                !targetTable ||
                                                selectedColumns.length === 0
                                                    ? "bg-gray-300 text-gray-500 cursor-not-allowed"
                                                    : "bg-green-500 text-white hover:bg-green-600"
                                            }`}
                                        >
                                            Ingest Data
                                        </button>
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>

            {/* Status Messages */}
            <div className="absolute right-4 bottom-8 rounded-xl border">
                {status && (
                    <div className="px-4 py-3 rounded text-sm">
                        <p className="font-medium">
                            Status:{" "}
                            <span className="font-normal">{status}</span>
                        </p>
                    </div>
                )}
            </div>
        </div>
    );
}

export default App;
