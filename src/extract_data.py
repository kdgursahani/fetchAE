import json
import gzip
import sqlite3

import json
import gzip

db_file = 'data.db'

def extract_data(file_path):
    """
    Extract data from a JSON Gzip file and return it as a list of dictionaries.
    Handles malformed lines by stripping out content before the first '{' and loading valid JSON data.
    """
    data = []
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            # Process each line
            for line in f:
                line = line.strip()
                if '{' in line and '}' in line:
                    # Find the first '{' and the last '}'
                    json_start = line.find('{')
                    json_end = line.rfind('}')
                    
                    # Extract the content between the first '{' and last '}'
                    valid_json_content = line[json_start:json_end + 1]
                    
                    # Try loading the JSON content
                    try:
                        data.append(json.loads(valid_json_content))
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}. Line content: {line}")
                else:
                    print(f"Skipping line without JSON: {line} in {file_path}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error extracting data from file {file_path}: {e}")
    
    return data

def infer_schema_from_data(data):
    """
    Infers schema from the receipt data by checking field names and types.
    """
    field_types = {}

    for receipt in data:
        if 'rewardsReceiptItemList' in receipt:
            for item in receipt['rewardsReceiptItemList']:
                for key, value in item.items():
                    # Determine the data type for each field
                    if isinstance(value, str):
                        field_type = 'TEXT'
                    elif isinstance(value, int):
                        field_type = 'INTEGER'
                    elif isinstance(value, float):
                        field_type = 'REAL'
                    else:
                        field_type = 'TEXT'  # Default to TEXT if type is unknown
                    
                    # Store or update the field type
                    if key not in field_types:
                        field_types[key] = field_type
                    elif field_types[key] != field_type:
                        field_types[key] = 'TEXT'  # Handle mixed types by defaulting to TEXT
    
    return field_types


def create_and_load_receipts_table(conn, data):
    """
    Create and load the receipts and receipt_items tables with dynamic schema inference for receipt_items.
    """
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS receipts''')
    cursor.execute('''DROP TABLE IF EXISTS receipt_items''')
    
    # Create the receipts table
    cursor.execute('''CREATE TABLE IF NOT EXISTS receipts (
                        _id TEXT PRIMARY KEY,
                        createDate TEXT,
                        dateScanned TEXT,
                        finishedDate TEXT,
                        modifyDate TEXT,
                        purchaseDate TEXT,
                        rewardsReceiptStatus TEXT,
                        totalSpent REAL,
                        userId TEXT
                    )''')

    # Infer the schema for receipt_items
    inferred_schema = infer_schema_from_data(data)
    
    # Construct the CREATE TABLE statement based on inferred schema
    receipt_items_fields = ['item_id INTEGER PRIMARY KEY AUTOINCREMENT', 'receipt_id TEXT']
    for field, field_type in inferred_schema.items():
        receipt_items_fields.append(f"{field} {field_type}")
    
    receipt_items_schema = ', '.join(receipt_items_fields)
    
    # Create the receipt_items table with the inferred schema
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS receipt_items ({receipt_items_schema}, 
                        FOREIGN KEY (receipt_id) REFERENCES receipts(_id))''')

    # Print the number of receipts we're about to load
    print(f"Attempting to load {len(data)} receipts...")
    total_item_count = 0
    
    # Load data into the receipts and receipt_items tables
    for receipt in data:
        _id = receipt['_id']['$oid'] if '_id' in receipt and '$oid' in receipt['_id'] else None
        createDate = receipt['createDate']['$date'] if 'createDate' in receipt and '$date' in receipt['createDate'] else None
        dateScanned = receipt['dateScanned']['$date'] if 'dateScanned' in receipt and '$date' in receipt['dateScanned'] else None
        finishedDate = receipt['finishedDate']['$date'] if 'finishedDate' in receipt and '$date' in receipt['finishedDate'] else None
        modifyDate = receipt['modifyDate']['$date'] if 'modifyDate' in receipt and '$date' in receipt['modifyDate'] else None
        purchaseDate = receipt['purchaseDate']['$date'] if 'purchaseDate' in receipt and '$date' in receipt['purchaseDate'] else None
        rewardsReceiptStatus = receipt.get('rewardsReceiptStatus')
        totalSpent = float(receipt.get('totalSpent', 0.0))
        userId = receipt.get('userId')

        # Insert data into the receipts table
        cursor.execute('''INSERT OR REPLACE INTO receipts (_id, createDate, dateScanned, finishedDate, modifyDate, purchaseDate,
                                                rewardsReceiptStatus, totalSpent, userId)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (_id, createDate, dateScanned, finishedDate, modifyDate, purchaseDate,
                        rewardsReceiptStatus, totalSpent, userId))

        # Insert data into the receipt_items table
        if 'rewardsReceiptItemList' in receipt:
            for item in receipt['rewardsReceiptItemList']:
                total_item_count += 1
                
                # Collect values for the dynamically inferred fields
                values = [_id]  # receipt_id is the first value
                for field in inferred_schema.keys():
                    values.append(item.get(field))  # Get the value for each field, default to None if not present

                # Build the SQL INSERT statement dynamically
                fields_str = ', '.join(['receipt_id'] + list(inferred_schema.keys()))
                placeholders = ', '.join(['?'] * len(values))
                cursor.execute(f'''INSERT OR REPLACE INTO receipt_items ({fields_str}) VALUES ({placeholders})''', values)

    conn.commit()

    # Confirm rows inserted into receipts
    cursor.execute('SELECT COUNT(_id) FROM receipts')
    receipts_count = cursor.fetchone()[0]
    print(f"Successfully loaded {receipts_count} receipts into the database.")
    print(f"Attemped to load {total_item_count} receipt items into the database.")
    # Confirm rows inserted into receipt_items
    cursor.execute('SELECT COUNT(item_id) FROM receipt_items')
    receipt_items_count = cursor.fetchone()[0]
    print(f"Successfully loaded {receipt_items_count} receipt items into the database.")


def create_and_load_brands_table(conn, data):
    """
    Create and load the brands table.
    """
    cursor = conn.cursor()

    # Create the brands table
    cursor.execute('''CREATE TABLE IF NOT EXISTS brands (
                        _id TEXT PRIMARY KEY,
                        barcode TEXT,
                        brandCode TEXT,
                        category TEXT,
                        categoryCode TEXT,
                        cpg TEXT,
                        topBrand INTEGER,
                        name TEXT
                    )''')

    # Print the number of brands we're about to load
    print(f"Attempting to load {len(data)} brands...")

    # Load data into the brands table
    for brand in data:
        _id = brand['_id']['$oid'] if '_id' in brand and '$oid' in brand['_id'] else None
        barcode = brand.get('barcode')
        brandCode = brand.get('brandCode')
        category = brand.get('category')
        categoryCode = brand.get('categoryCode')
        cpg = None
        if 'cpg' in brand:
            cpg_ref = brand['cpg'].get('$ref', '')
            cpg_id = brand['cpg'].get('$id', {}).get('$oid', '')
            cpg = f"{cpg_ref}: {cpg_id}"
        topBrand = int(brand.get('topBrand', 0)) if 'topBrand' in brand else 0
        name = brand.get('name')

        cursor.execute('''INSERT OR REPLACE INTO brands (_id, barcode, brandCode, category, categoryCode, cpg, topBrand, name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (_id, barcode, brandCode, category, categoryCode, cpg, topBrand, name))

    conn.commit()

    # Confirm rows inserted into brands
    cursor.execute('SELECT COUNT(_id) FROM brands')
    brands_count = cursor.fetchone()[0]
    print(f"Successfully loaded {brands_count} brands into the database.")


def create_and_load_users_table(conn, data):
    """
    Create and load the users table.
    """
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        _id TEXT PRIMARY KEY,
                        state TEXT,
                        createdDate TEXT,
                        lastLogin TEXT,
                        role TEXT,
                        active INTEGER
                    )''')

    # Print the number of users we're about to load
    print(f"Attempting to load {len(data)} users...")

    # Load data into the users table
    for user in data:
        _id = user['_id']['$oid'] if '_id' in user and '$oid' in user['_id'] else None
        state = user.get('state') if 'state' in user else None
        createdDate = user['createdDate']['$date'] if 'createdDate' in user and '$date' in user['createdDate'] else None
        lastLogin = user['lastLogin']['$date'] if 'lastLogin' in user and '$date' in user['lastLogin'] else None
        role = user.get('role') if 'role' in user else None
        active = int(user.get('active', 0))  # Convert to 0/1 for SQLite BOOLEAN

        cursor.execute('''INSERT OR REPLACE INTO users (_id, state, createdDate, lastLogin, role, active)
                          VALUES (?, ?, ?, ?, ?, ?)''',
                       (_id, state, createdDate, lastLogin, role, active))

    conn.commit()

    # Confirm rows inserted into users
    cursor.execute('SELECT COUNT(_id) FROM users')
    users_count = cursor.fetchone()[0]
    print(f"Successfully loaded {users_count} users into the database.")

def main():
    # Set the path to the receipts JSON Gzip file
    filepaths =  {
        'receipts_file_path' : 'receipts.json.gz',
        'users_file_path': 'users.json.gz',
        'brands_file_path' : 'brands.json.gz'}

    # Initialize SQLite database
    db_file = 'data.db'
    conn = sqlite3.connect(db_file)

    try:
        # Extract and Load each one of the tables
        # brands
        brandsData = extract_data(filepaths['brands_file_path'])
        create_and_load_brands_table(conn, brandsData)
        
        # receipts
        receiptsData = extract_data(filepaths['receipts_file_path'])
        create_and_load_receipts_table(conn, receiptsData)
        
        # users
        usersData = extract_data(filepaths['users_file_path'])
        create_and_load_users_table(conn, usersData)
        
        # Execute SQL Query
        print("Executing SQL Query 1...")
        cursor = conn.cursor()
        cursor.execute(
        '''
        SELECT rewardsReceiptStatus, AVG(totalSpent) AS average_spend
        FROM receipts
        WHERE rewardsReceiptStatus IN ('REJECTED', 'FINISHED')
        GROUP BY rewardsReceiptStatus;''')
        # Fetch and print results
        result = cursor.fetchall()
        for row in result:
            print(f"Rewards Receipt Status: {row[0]}, Average Spend: {row[1]}")
            # print(f"Brand Code: {row[0]}, Receipt Count: {row[1]}")
                # Execute SQL Query

        print("Executing SQL Query 2...")
        cursor = conn.cursor()
        cursor.execute(
        '''
        WITH filtered_receipts AS (
        SELECT _id, rewardsReceiptStatus
        FROM receipts
        WHERE rewardsReceiptStatus IN ('REJECTED', 'FINISHED')
        )
        SELECT 
            rewardsReceiptStatus, 
            SUM(ri.quantityPurchased) AS total_items_purchased
        FROM 
            receipt_items ri
        JOIN 
            filtered_receipts fr ON ri.receipt_id = fr._id
        GROUP BY 
            rewardsReceiptStatus
        ORDER BY 
            total_items_purchased DESC;
        ''')
        # Fetch and print results
        result = cursor.fetchall()
        for row in result:
            print(f"Rewards Receipt Status: {row[0]}, Total Items Purchased: {row[1]}")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        # Close database connection
        if conn:
            conn.close()

def get_table_schema(conn, table_name):
    """
    Retrieves the schema of the specified table.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    schema_info = cursor.fetchall()
    
    print(f"Schema for {table_name}:")
    for column in schema_info:
        column_id, column_name, column_type, not_null, default_val, is_pk = column
        print(f"Column: {column_name}, Type: {column_type}, Not Null: {not_null}, Primary Key: {is_pk}")

def get_db_conn():
    return sqlite3.connect(db_file)

if __name__ == "__main__":
    # Example usage
    main()
    conn = get_db_conn()
    conn.close()

