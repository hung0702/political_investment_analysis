# Political Investment Analysis

This project aims to analyze the financial transactions of members of the U.S. Congress as disclosed in the House Stock Watcher and Senate Stock Watcher databases. It extracts, transforms, and loads transaction data into a structured SQL database, then fetches historical stock price information to enable comprehensive analysis of their investment activities.

## How It Works

1.  **Extract:** Fetches raw transaction data from CSV sources and stock price data using the yfinance API.
2.  **Transform:** Cleans, standardizes, and validates the data. This includes parsing dates, cleaning ticker symbols, and handling inconsistencies.
3.  **Load:** Loads the transformed data into the SQL database, creating the following tables:
    *   `senate_transactions`, `house_transactions`: Store individual transactions.
    *   `price_data`: Stores historical stock price data.
    *   `all_transactions_priced`: Combines transaction and price data for analysis.
4.  **Manual Corrections:** Applies SQL scripts to fix any remaining data issues identified through manual review.

## Getting Started

1.  **Prerequisites:**
    *   Python 3.11
    *   PostgreSQL database
2.  **Setup:**
    *   Clone this repository.
    *   Create a `.env` file in the project root and set your database connection details (e.g., `DATABASE_URL=postgresql://user:password@host:port/database_name`).
    *   Install dependencies using `pip install -r requirements.txt`.
3.  **Run:**
    *   Navigate to the project root directory.
    *   First execute `python scripts/transactions.py` to load transaction data.
    *   Then execute `python scripts/prices.py` to fetch and load price data.

## Project Structure

The project follows an ETL (Extract, Transform, Load) pattern and is organized as follows:

*   **`config.py`:** Stores configuration variables like CSV data source URLs.
*   **`db/`:**
    *   **`connection.py`:** Handles database connection setup.
    *   **`manual_corrections/`:** Contains SQL scripts for manual data corrections.
    *   **`tables/`:** SQL scripts to define the database schema (`transactions.sql`, `price_data.sql`, `all_transactions_priced.sql`).
*   **`extract/`:**
    *   **`get_transactions.py`:** Fetches raw transaction data from CSV files.
    *   **`get_price_data.py`:** Fetches historical stock price data using the `yfinance` library.
*   **`load/`:**
    *   **`transactions_to_db.py`:** Loads transformed transaction data into the database.
    *   **`price_data_to_db.py`:** Loads fetched price data into the database.
*   **`transform/`:**
    *   **`transform_transactions.py`:** Cleans and standardizes raw transaction data.
*   **`scripts/`:**
    *   **`transactions.py`:** Main script to run the ETL pipeline for transactions.
    *   **`prices.py`:** Main script to run the price fetching and loading process.
*   **`tests/`:** (Empty in the provided structure, but intended for unit tests)
*   **`README.md`:** This project documentation.
*   **`requirements.txt`:** Lists project dependencies.
