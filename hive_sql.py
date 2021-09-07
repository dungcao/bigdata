from pyhive import hive
from TCLIService.ttypes import TOperationState

cursor = hive.connect('localhost', port=10000).cursor()
# cursor.execute("""SELECT * FROM tbl_employees where address.city like 'Ho%'""")

cursor.execute("""SELECT * FROM tbl_employees where address.city like 'Ho%'""", async=True)

status = cursor.poll().operationState
while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    logs = cursor.fetch_logs()
    for message in logs:
        print(message)

    # If needed, an asynchronous query can be cancelled at any time with:
    # cursor.cancel()

    status = cursor.poll().operationState

for row in cursor.fetchall():
    print(row)