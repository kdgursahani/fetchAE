import os
from extract_data import extract_data, create_and_load_receipts_table, create_and_load_brands_table, create_and_load_users_table
from db_conn import get_db_conn
from sql_queries import get_receipts_status_avg_query, get_items_purchased_query

def main():
    print("Current Working Directory:", os.getcwd())
    receipts_file_path = "data/receipts.json.gz"
    brands_file_path = "data/brands.json.gz"
    users_file_path = "data/users.json.gz"

    receipts_data = extract_data(receipts_file_path)
    brands_data = extract_data(brands_file_path)
    users_data = extract_data(users_file_path)

    conn = get_db_conn()  # No arguments needed

    create_and_load_receipts_table(conn, receipts_data)
    create_and_load_brands_table(conn, brands_data)
    create_and_load_users_table(conn, users_data)

    cursor = conn.cursor()

    # Executing and printing results of queries
    cursor.execute(get_receipts_status_avg_query())
    avg_spending = cursor.fetchall()
    print("Average spending by status:", avg_spending)

    cursor.execute(get_items_purchased_query())
    items_purchased = cursor.fetchall()
    print("Items purchased:", items_purchased)
    conn.close()

if __name__ == "__main__":
    main()
