# data_quality_report.py

import pandas as pd
import argparse
from extract_data import get_db_conn

# function to calculate and return missing values in each column
def check_missing_values(df):
    """
    check for missing values in each column and return a DataFrame with counts and percentages
    """
    missing_values = df.isnull().sum()  # counting null values
    missing_percent = (missing_values / len(df)) * 100  # calculate percentage of missing values
    return pd.DataFrame({'Missing Values': missing_values, 'Percentage Missing': missing_percent})

# function to read table from SQLite database and convert to pandas DataFrame
def read_table_from_db(conn, table_name):
    """
    fetches table data from SQLite database and loads it into a pandas DataFrame
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, conn)

# function to generate a missing values report
def generate_missing_values_report(df):
    """
    generate a comprehensive missing values report
    """
    report = {}
    report['Missing Values Report'] = check_missing_values(df)
    return report

# function to save report data into excel
def save_report_to_csv(report, output_file):
    """
    save the missing values report to an Excel file
    """
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        report['Missing Values Report'].to_excel(writer, sheet_name='Missing Values')

    print(f"Missing values report saved to {output_file}")

# main function to run the analysis
def main():
    # set up argument parsing for input table and output file
    parser = argparse.ArgumentParser(description="Generate a Missing Values Report.")
    parser.add_argument('--table', type=str, required=True, help="Table name from SQLite database.")
    parser.add_argument('--output', type=str, required=True, help="Path to save the missing values report.")

    args = parser.parse_args()

    # get the database connection from extract_data.py
    conn = get_db_conn()

    if conn is None:
        print("Failed to connect to database")
        return

    # load the table data
    df = read_table_from_db(conn, args.table)

    # generate the missing values report
    report = generate_missing_values_report(df)

    # save the report to a CSV file
    save_report_to_csv(report, args.output + "_missing_values_report.xlsx")

    conn.close()

if __name__ == "__main__":
    main()
