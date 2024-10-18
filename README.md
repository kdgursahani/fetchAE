# Fetch Analytics Engineering
The following project resolves unstructured data into well-defined tables in a sqlite3 database, and then performs querying to find answers to important business questions as well as identify any data quality issues present in the dataset provided. I had a lot of fun doing this.

## Table of Contents

0. [Review]
1. [Stage1]
2. [Stage2]
3. [Stage3]
4. [Stage4]

## Review

## Stage1
You can verify the relational data model in the 'Relational Data Model & Summary.pdf'
The main application is located in `src/main.py`. To run it, use the following command:

```bash
# Run the main application
python src/main.py
```
This will help perform the extraction of data into a database with four tables: receipts, rececipt_items, brands, and users.

## Stage2
This will also print the results for two of the questions asked (NOTE: This assumes that 'Accepted' = 'Finished')
  - When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Finished’ or ‘Rejected’, which is greater?
When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
  - When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Finished’ or ‘Rejected’, which is greater?
When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
