# main.py

import sqlite3
import sys
import argparse
from extract_data import get_table_schema, get_db_conn

# Command-line argument parser setup
parser = argparse.ArgumentParser(description="Retrieve table schema information from an SQLite database.")
parser.add_argument('--table', type=str, required=True, choices=['receipts', 'receipt_items', 'brands', 'users'],
                    help="Specify the table name ('receipts', 'receipt_items', 'brands', or 'users')")

# Parse the arguments
args = parser.parse_args()

# Connect to the SQLite database (replace 'your_database.db' with the actual database path)
conn = get_db_conn()
# Call the function to get the schema based on the flag
get_table_schema(conn, args.table)
# Close the connection
conn.close()
