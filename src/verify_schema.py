import argparse
from extract_data import get_table_schema
from db_conn import get_db_conn

# Command-line argument parser setup
parser = argparse.ArgumentParser(description="Retrieve table schema information from an SQLite database.")
parser.add_argument('--table', type=str, required=True, choices=['receipts', 'receipt_items', 'brands', 'users'],
                    help="Specify the table name ('receipts', 'receipt_items', 'brands', or 'users')")

# Parse the arguments
args = parser.parse_args()

# Connect to the SQLite database (replace 'your_database.db' with the actual database path)
conn = get_db_conn()
cursor = conn.cursor()
# Call the function to get the schema based on the flag
cursor.execute(get_table_schema(args.table))
results = cursor.fetchall()
print(results)
# Close the connection
conn.close()
