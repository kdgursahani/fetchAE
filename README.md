# Fetch Analytics Engineering
The following project resolves unstructured data into well-defined tables in a sqlite3 database, and then performs querying to find answers to important business questions as well as identify any data quality issues present in the dataset provided. I had a lot of fun doing this.

## Stage1
You can verify the relational data model in 'Relational Data Model & Summary.pdf'.

Since receipts.json consisted of a list of receipt items as a field within a given receipt object, for the ease of future analysis of data, it was decided that receipt_items would have its own table.

The main application is located in `src/main.py`. To run it, use the following command:

```bash
# Run the main application
python src/main.py
```
This will help perform the extraction of data into a database with four tables: receipts, rececipt_items, brands, and users. The logic for extraction of data from `*.json.gz` is located within `extract_data.py`. Print statements have been placed strategically throughout `extract_data.py` to ensure that verification of loads can take place. Database connections are formed and maintained through `get_db_conn()` located in `db_conn.py`. Additionally, `extract_data.py` consists of a function called `get_table_schema` that allows us to retrieve the schema of each one of the tables loaded into `data.db`. This can be of help to us while trying to verify the load of data into our database.

You can also verify the schema of each of the tables using the following command. (NOTE: A flag --table is required and it accepts the values of the table names)
```bash 
# Run verify_schema
python src/verify_schema.py --table 'receipts'
```

## Stage2
Running `python src/main.py` will also print the results for two of the questions asked (NOTE: This assumes that 'Accepted' = 'Finished' as there were no instances of 'Accepted' observed. These queries are located in `business_sql_queries.sql`)
  - When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Finished’ or ‘Rejected’, which is greater?
    ```bash
    Average spending by status: [('FINISHED', 80.85430501930502), ('REJECTED', 23.326056338028184)]
    ```
    We can see here that 'Finished/Accepted' receipts have a higher average spending compared to 'Rejected' receipts.

- When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Finished’ or ‘Rejected’, which is greater?
    ```bash
    Items purchased: [('FINISHED', 5918), ('REJECTED', 164)]
    ```
    We can see here that 'Finished/Accepted' receipts have a higher total number of items purchased compared to 'Rejected' ones.

## Stage3
Various data quality issues were investigated. First, using `missing_values_report.py`, a programmatic extractor for reporting missing values across each of the tables was created. Missing or null values can be a 
 data concern. One can run a missing values report using the following command, a simple analysis exists in the *.xlsx files for brands and receipts within `data_quality_reports`.
```bash
python src/missing_values_report.py --table 'receipts' --output 'data_quality_reports/receipts'
```

Additionally, if you run the command `python src/data_quality_sql.py`, it will execute queries that take a deeper-dive into some of the data quality issues that I was interested in looking into.
```bash
python src/data_quality_sql.py
```
Summary of these data quality issues:
- There were 448 instances of null purchase dates. This likely points to not being able to effectively scan/recognize the date present on the receipt.
- There were 71 with no receipt scanned at all. This is less of a data quality issue and more of a user-engagement problem. But it points to an interesting business case.
- There were 40 users that had never logged-in. This is also a user-engagement problem. (This could also likely be fetch staff, which probably should be ignored in any future analysis)
- Unusual spends were seen (Based of the heuristic I used (totalSpent is more than twice of the average spend)).
- There were 148 receipts that had no associated user within the users table. Does the users table consist of users with non-deleted accounts only? Were these receipts issued by non-users? These all would be good questions to ask as a follow-up.

## Stage4
Review Data Quality Insights and Next Steps.pdf for the email to the business stakeholder! 
