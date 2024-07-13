# politicians_investment_analysis
 
## Overview
This project aims to analyze stock trading activities of politicians, connecting their investments to legislative activities and possible influences. By collecting data from various public sources, this initiative seeks to identify patterns and potential conflicts of interest between political decisions and personal financial gains.

## Data Sources
The primary sources of stock transaction data are the House and Senate stock watchers. These platforms provide insights into the trading activities of US politicians.

### Completed
- Import data from the House Stock Watcher.
- Import data from the Senate Stock Watcher.

### To-Do
- Connect to congressional disclosure databases and use OCR technology to capture transaction data more accurately.
- Extend data mining to include family member trades.
- Link tickers to their respective sectors and industries for enhanced accuracy.

## Issues Encountered
- Some transaction fields, despite appearing on disclosure reports, were not accurately captured. For example, transactions involving Michael F. Bennet on 5/16/2023 were problematic.

## Additional Data Sources
- **SEC Edgar Forms 4 & 5**: Transactions by investors owning more than 10%, company directors, and executives.
- **Executive Trade Data**: Focus on transaction type, number of shares, price, transaction date, and post-transaction share total.

## Data Transformation
- **Setup PostgreSQL**: Establish a database to store and manage the transaction data.
- **Data Schema**: Use a model to unify the House and Senate data structures, treating all politicians' trades uniformly.
- **Analysis Goals**:
  - Investigate if politicians are more likely to invest in sectors related to recent legislative activities.
  - Examine the timing of investments relative to committee assignments.
  - Analyze the frequency and types of sectors in which politicians invest.

## Current Progress
- PostgreSQL setup and initial connections have been established.
- Basic data ingestion scripts for the House and Senate transaction data have been implemented.

## Future Enhancements
- Improve data accuracy by integrating OCR to capture transaction data from scanned PDFs.
- Expand the dataset to include trades reported by family members.
- Develop a comprehensive analysis framework to study the influence of legislative activities on investment decisions.

---

## Installation and Setup

### Requirements
- Python 3.8+
- PostgreSQL
- Required Python libraries: `psycopg2`, `requests`, `python-dotenv`

### Setup
1. Clone the repository:
