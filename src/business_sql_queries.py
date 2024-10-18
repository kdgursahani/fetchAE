# sql queries for business questions
def get_receipts_status_avg_query():
    print("Generating answer to the question :\n")
    print("When considering average spend from receipts with" +
          " 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?\n")
    return '''
        SELECT rewardsReceiptStatus, AVG(totalSpent) AS average_spend
        FROM receipts
        WHERE rewardsReceiptStatus IN ('REJECTED', 'FINISHED')
        GROUP BY rewardsReceiptStatus;
    '''

def get_items_purchased_query():
    print("Generating answer to the question :\n")
    print("When considering total number of items purchased from receipts with"+ 
          " 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?")
    return '''
        SELECT 
            rewardsReceiptStatus,
            COUNT(receipt_items.item_id) AS item_count
        FROM receipts
        JOIN receipt_items ON receipts._id = receipt_items.receipt_id
        WHERE rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
        GROUP BY rewardsReceiptStatus;
    '''
