# Politicians Investment Analysis
 
## Overview
Analyze stock trading activities of politicians and connect to their legislative activities to establish baseline for all politicians, and identify potential conflicts of interest arising from participation in legislative bodies.

## Data Sources
Stock transaction data are the House and Senate stock watchers. This data is mined from congressional disclosure reports. Eventually, functionality will be built to mine directly from public disclosure reports.

### Current Progress
- Basic data ingestion scripts for the House and Senate transaction data have been implemented.
- Raw data tables for House and for Senate

### To-Do
- Schema to tabulate House/Senate transactions
 - Additional tables to handle ticker sectors/industry, price at time of transaction
- Use NASDAQ data for tickers for enhanced accuracy.
- Use Yahoo Finance or other historical stock price data to track gains/losses
- Transition data collection and processing back to Go for improved performance and maintenance.
- Develop an Apache Airflow script to orchestrate daily updates and handle dependencies between data collection, processing, and storage.
- Connect to congressional disclosure databases, OCR/tuning, logic to handle amendments and various report types.

## Issues Encountered
- House and Senate Stock Watcher APIs are not comprehensive, nor are they always accurate.

## Additional Data Sources to Integrate
~- **SEC Edgar Forms 4 & 5**: Transactions by investors owning more than 10%, company directors, and executives.~  
~- **Executive Trade Data**: Focus on transaction type, number of shares, price, transaction date, and post-transaction share total.~

## Data Transformation
- Model House and Senate data structures, treating all politicians' trades uniformly.
- **Analysis Goals**:
  - Investigate if politicians are more likely to invest in sectors related to their recent legislative activities.
  - Examine the timing of investments relative to committee assignments.
  - Analyze the frequency and types of sectors in which politicians invest.
