import json
import gzip

def extract_data(file_path):
    """
    Extract data from a JSON Gzip file and return it as a list of dictionaries.
    Handles malformed lines by stripping out content before the first '{' and loading valid JSON data.
    """
    data = []
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if '{' in line and '}' in line:
                    json_start = line.find('{')
                    json_end = line.rfind('}')
                    valid_json_content = line[json_start:json_end + 1]
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
                    if isinstance(value, str):
                        field_type = 'TEXT'
                    elif isinstance(value, int):
                        field_type = 'INTEGER'
                    elif isinstance(value, float):
                        field_type = 'REAL'
                    else:
                        field_type = 'TEXT'
                    
                    if key not in field_types:
                        field_types[key] = field_type
                    elif field_types[key] != field_type:
                        field_types[key] = 'TEXT'
    
    return field_types

def create_and_load_receipts_table(conn, data):
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS receipts''')
    cursor.execute('''DROP TABLE IF EXISTS receipt_items''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS receipts (
                        _id TEXT PRIMARY KEY,
                        bonusPointsEarned REAL,
                        bonusPointsEarnedReason TEXT,
                        createDate TEXT,
                        dateScanned TEXT,
                        finishedDate TEXT,
                        modifyDate TEXT,
                        purchaseDate TEXT,
                        pointsAwardedDate TEXT,
                        pointsEarned REAL,
                        purchasedItemCount REAL,
                        rewardsReceiptStatus TEXT,
                        totalSpent REAL,
                        userId TEXT
                    )''')

    inferred_schema = infer_schema_from_data(data)
    receipt_items_fields = ['item_id INTEGER PRIMARY KEY AUTOINCREMENT', 'receipt_id TEXT']
    for field, field_type in inferred_schema.items():
        receipt_items_fields.append(f"{field} {field_type}")
    
    receipt_items_schema = ', '.join(receipt_items_fields)
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS receipt_items ({receipt_items_schema}, 
                        FOREIGN KEY (receipt_id) REFERENCES receipts(_id))''')

    print(f"Attempting to load {len(data)} receipts...")
    total_item_count = 0
    
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

        cursor.execute('''INSERT OR REPLACE INTO receipts (_id, createDate, dateScanned, finishedDate, modifyDate, purchaseDate,
                                                rewardsReceiptStatus, totalSpent, userId)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (_id, createDate, dateScanned, finishedDate, modifyDate, purchaseDate,
                        rewardsReceiptStatus, totalSpent, userId))

        if 'rewardsReceiptItemList' in receipt:
            for item in receipt['rewardsReceiptItemList']:
                total_item_count += 1
                values = [_id]
                for field in inferred_schema.keys():
                    values.append(item.get(field))

                fields_str = ', '.join(['receipt_id'] + list(inferred_schema.keys()))
                placeholders = ', '.join(['?'] * len(values))
                cursor.execute(f'''INSERT OR REPLACE INTO receipt_items ({fields_str}) VALUES ({placeholders})''', values)

    conn.commit()

    cursor.execute('SELECT COUNT(_id) FROM receipts')
    receipts_count = cursor.fetchone()[0]
    print(f"Successfully loaded {receipts_count} receipts into the database.")
    cursor.execute('SELECT COUNT(item_id) FROM receipt_items')
    receipt_items_count = cursor.fetchone()[0]
    print(f"Successfully loaded {receipt_items_count} receipt items into the database.")

def create_and_load_brands_table(conn, data):
    cursor = conn.cursor()

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

    print(f"Attempting to load {len(data)} brands...")

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

    cursor.execute('SELECT COUNT(_id) FROM brands')
    brands_count = cursor.fetchone()[0]
    print(f"Successfully loaded {brands_count} brands into the database.")

def create_and_load_users_table(conn, data):
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        _id TEXT PRIMARY KEY,
                        state TEXT,
                        createdDate TEXT,
                        lastLogin TEXT,
                        role TEXT,
                        active INTEGER
                    )''')

    print(f"Attempting to load {len(data)} users...")

    for user in data:
        _id = user['_id']['$oid'] if '_id' in user and '$oid' in user['_id'] else None
        state = user.get('state') if 'state' in user else None
        createdDate = user['createdDate']['$date'] if 'createdDate' in user and '$date' in user['createdDate'] else None
        lastLogin = user['lastLogin']['$date'] if 'lastLogin' in user and '$date' in user['lastLogin'] else None
        role = user.get('role') if 'role' in user else None
        active = int(user.get('active', 0))

        cursor.execute('''INSERT OR REPLACE INTO users (_id, state, createdDate, lastLogin, role, active)
                          VALUES (?, ?, ?, ?, ?, ?)''',
                       (_id, state, createdDate, lastLogin, role, active))

    conn.commit()

    cursor.execute('SELECT COUNT(_id) FROM users')
    users_count = cursor.fetchone()[0]
    print(f"Successfully loaded {users_count} users into the database.")

def get_table_schema(table_name):
    return f'''
        SELECT 
            name AS column_name,
            type AS column_type,
            dflt_value AS default_value
        FROM pragma_table_info('{table_name}');
    '''
