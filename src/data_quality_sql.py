from db_conn import get_db_conn

# SQL queries for data quality issues

def get_receipts_with_missing_fields_query():
    # This query counts the number of receipts with missing fields (userId, purchaseDate, totalSpent).
    return '''
        SELECT 
            COUNT(*) AS missing_fields_count,
            SUM(CASE WHEN userId IS NULL THEN 1 ELSE 0 END) AS null_userId_count,
            SUM(CASE WHEN purchaseDate IS NULL THEN 1 ELSE 0 END) AS null_purchaseDate_count,
            SUM(CASE WHEN totalSpent IS NULL THEN 1 ELSE 0 END) AS null_totalSpent_count
        FROM receipts
        WHERE userId IS NULL 
           OR purchaseDate IS NULL 
           OR totalSpent IS NULL;
    '''

def get_duplicate_receipts_query():
    # This query identifies duplicate receipts and counts them based on userId, purchaseDate, and totalSpent.
    return '''
        SELECT 
            userId,
            purchaseDate,
            totalSpent,
            COUNT(*) AS receipt_count
        FROM receipts
        GROUP BY userId, purchaseDate, totalSpent
        HAVING COUNT(*) > 1;  -- Only return groups with more than one receipt
    '''

def get_inactive_users_with_recent_receipts_query():
    # This query summarizes inactive users who have received receipts in the last 30 days.
    return '''
        SELECT 
            u._id AS user_id,
            COUNT(r._id) AS recent_receipts_count,
            MAX(r.createDate) AS last_receipt_date
        FROM users u
        LEFT JOIN receipts r ON u._id = r.userId
        WHERE u.active = 0
        AND r.createDate >= DATE('now', '-30 days')
        GROUP BY u._id;  -- Group by user to count receipts per user
    '''

def get_users_with_no_receipts_query():
    # This query counts users who have never received a receipt.
    return '''
        SELECT 
            COUNT(u._id) AS users_with_no_receipts
        FROM users u
        LEFT JOIN receipts r ON u._id = r.userId
        WHERE r._id IS NULL;  -- Users without receipts
    '''

def get_receipts_with_unusual_total_amounts_query():
    # This query identifies receipts with total amounts that are significantly higher or lower than the average.
    return '''
        WITH average_spent AS (
            SELECT AVG(totalSpent) AS avg_spent
            FROM receipts
        )
        SELECT 
            COUNT(*) AS unusual_receipts_count,
            SUM(totalSpent) AS unusual_total_spent
        FROM receipts
        WHERE totalSpent > (SELECT avg_spent FROM average_spent) * 2
           OR totalSpent < (SELECT avg_spent FROM average_spent) / 10;
    '''

def get_users_who_have_never_logged_in_query():
    # This query counts users who have never logged into the application.
    return '''
        SELECT 
            COUNT(u._id) AS users_never_logged_in
        FROM users u
        WHERE lastLogin IS NULL;  -- Users with no recorded login
    '''

if __name__ == "__main__":
    # List of queries to run
    queries = {'Receipts with missing fields': get_receipts_with_missing_fields_query(),
    'Duplicate Receipts':  get_duplicate_receipts_query(),
    'Inactive Users with Recent Receipts':  get_inactive_users_with_recent_receipts_query(),
    'Users with no receipts':    get_users_with_no_receipts_query(),
    'Unusual receipt amounts':    get_receipts_with_unusual_total_amounts_query(),
    'Users never logged in, oh the irony':    get_users_who_have_never_logged_in_query(),
    }

    for query in queries:
        print(f'Executing query: {query}')
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute(queries[query])
        results = cursor.fetchall()
        print(results)
