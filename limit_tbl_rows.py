'''
python3 /opt/lampp/htdocs/sp_api_local/limit_tbl_rows.py
'''

import os
import sqlite3
from pprint import pprint

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

'''
Code only runs if the total number of table rows is not less than 'trigger_size'.
The number of table rows gets reduced to the 'max_rows' size.
This is done by deleting the first rows in the table.

The database filesize then gets reduced by running the 'VACUUM' statement (compact).

Eg. When 'trigger_size' = 6000 and 'max_rows' = 5000, if the table's total number
    of rows was 6040, the first 1040 rows would be deleted.
    If total number of rows was less than 6000 then nothing would happen.
'''
def limit_tbl_rows(trigger_size, max_rows, db_name, tbl) -> bool:
    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()
    
    sql = f"SELECT `rowid` FROM `{tbl}` ORDER BY `rowid`"
    db_cur.execute(sql)
    rowids = [row[0] for row in db_cur.fetchall()]
    total_rows = len(rowids)
    extra_rows = total_rows - max_rows

    if total_rows < trigger_size:
        pprint({'Total deleted rows: ': 'None'})
        return None
    
    pprint({'Total deleted rows: ': extra_rows})

    unwanted_rowids = []
    for i, rowid in enumerate(rowids):
        if i > extra_rows - 1:
            break
        unwanted_rowids.append(str(rowid))
    in_clause = "','".join(unwanted_rowids)

    sql = f"DELETE FROM `{tbl}` WHERE `rowid` IN ('{in_clause}')"
    db_cur.execute(sql)

    # commit() is only required for SQL statements that modify the database.
    db_conn.commit()
    db_cur.close()
    db_conn.close()

    compact_api_labels(db_name)

    return True

def compact_api_labels(db_name) -> None:
    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()

    db_cur.execute("VACUUM")

    db_conn.commit()
    db_cur.close()
    db_conn.close()


'''
Code runs when total rows equal or exceed 6000.
Delete x rows to leave 5000 rows.
'''
# limit_tbl_rows(1500, 1000, 'api_labels.db3', 'prime_labels')
limit_tbl_rows(6000, 5000, 'api_labels.db3', 'prime_labels')


'''
from datetime import datetime

# Get the current date and time
# current_datetime = datetime.now()

# Format the date and time as "yyyymmddhhmm"
datetime = datetime.now().strftime('%Y%m%d%H%M')
print(datetime)
'''
