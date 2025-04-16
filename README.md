# ClickHouse Flatfile Ingestion Tool

A web-based application for transferring data between ClickHouse databases and flat files (CSV/TXT). This tool simplifies the process of importing data from CSV/TXT files into ClickHouse and exporting data from ClickHouse tables to CSV files.

## Features

-   **Dual Data Source Support:**
    -   Connect to ClickHouse databases
    -   Upload and process CSV/TXT flat files
    -   Special handling for UK property price data files
-   **Data Management:**
    -   Browse ClickHouse tables and columns
    -   Preview data before and after ingestion
    -   Select specific columns for transfer
    -   Create new tables or insert into existing ones
-   **User-Friendly Interface:**
    -   Intuitive web-based UI built with React
    -   Simple connection configuration
    -   Column selection with select/deselect all options
    -   Status messages and error handling

## Project Structure

```
├── client/                 # Frontend React application
│   ├── src/                # React source code
│   ├── public/             # Public assets
│   ├── Dockerfile          # Docker configuration for client
│   └── package.json        # Frontend dependencies
│
├── server/                 # Backend Node.js application
│   ├── server.js           # Express server implementation
│   ├── uploads/            # Temporary storage for uploaded files
│   ├── Dockerfile          # Docker configuration for server
│   └── package.json        # Backend dependencies
│
└── clickhouse/             # ClickHouse configuration
    ├── docker-compose.yml  # Docker Compose setup for ClickHouse
    └── sample data files   # (pp-2023.txt, pp-2024.csv)
```

## Setup and Installation

### Prerequisites

-   Node.js and npm
-   Docker and Docker Compose (for ClickHouse setup)

### Running ClickHouse

1. Navigate to the clickhouse directory:

    ```
    cd clickhouse
    ```

2. Start ClickHouse using Docker Compose:

    ```
    docker-compose up -d
    ```

3. ClickHouse will be available at:
    - HTTP interface: http://localhost:8123
    - Native interface: localhost:9000

### Running the Backend

1. Navigate to the server directory:

    ```
    cd server
    ```

2. Install dependencies:

    ```
    npm install
    ```

3. Start the server:

    ```
    node server.js
    ```

4. The server will run on http://localhost:8000

### Running the Frontend

1. Navigate to the client directory:

    ```
    cd client
    ```

2. Install dependencies:

    ```
    npm install
    ```

3. Start the development server:

    ```
    npm run dev
    ```

4. The client will be available at http://localhost:5173

## Usage Guide

### Connecting to ClickHouse

1. Select "ClickHouse" as the source type
2. Enter connection details (defaults: host=localhost, port=8123, database=default, user=default)
3. Click "Connect"
4. Select a table from the dropdown and click "Load Columns"

### Working with Flat Files

1. Select "Flat File" as the source type
2. Upload a CSV or TXT file
3. The system will automatically detect columns

### Ingesting Data

1. Select columns you want to include in the transfer
2. Enter a target table name
3. Click "Ingest Data"
4. Once ingestion is complete, you can preview or download the data

### UK Property Price Data

The system has special handling for UK property price data files (files with names containing "pp-" or "price-paid"). It will automatically map to standard column names:

-   transaction_id
-   price
-   date_of_transfer
-   postcode
-   property_type
-   old_new
-   duration
-   paon
-   saon
-   street
-   locality
-   town_city
-   district
-   county
-   ppd_category_type
-   record_status

## Docker Support

Both the client and server components have Dockerfiles for containerization. The ClickHouse instance is already configured to run in Docker using the provided docker-compose.yml file.

## Technologies Used

-   **Frontend:**

    -   React
    -   Axios for HTTP requests
    -   CSS for styling

-   **Backend:**

    -   Node.js
    -   Express
    -   Multer for file uploads
    -   CSV-Parse for CSV processing
    -   ClickHouse Node.js client

-   **Database:**
    -   ClickHouse

## License

This project was created as a college project for Semester 6.

---

Created by [Your Name]
