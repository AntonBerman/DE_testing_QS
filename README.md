# DE_testing_QS
test case for QS

RUN pip install -r requirements.txt

RUN python build_database.py

OUTPUT
>Defined next file in transactions: budapest.csv: budapest

>Uploaded transactions : budapest.csv: True

>Defined next file in transactions: london_transactions.csv: london

>Uploaded transactions : london_transactions.csv: True

>Defined next file in transactions: ny.csv: new york

>Uploaded transactions : ny.csv: True

>Total values of transactions in DB: 150000

>Defined next file in stocks: bar_data.csv

>Uploaded stock : bar_data.csv: True

>Total values of stocks in DB: 93

>Starting updating glass_type_id through 238 values

>Received 238 values from API

>Updated glasses types for drinks: True

>Got and Saved next general stats table: (710, 5):general_stats_for_20230616.xlsx

>Got and Saved next procurement table: (388, 3):need_for_procurement_for_20230616.xlsx