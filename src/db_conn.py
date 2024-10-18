import sqlite3

def get_db_conn():
    """
    Returns a connection to the SQLite database.
    """
    db_path = 'data.db'
    conn = sqlite3.connect(db_path)
    return conn
