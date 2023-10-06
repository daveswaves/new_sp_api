'''
python3 /opt/lampp/htdocs/sp_api_local/limit_tbl_rows.py
'''

import os
import sqlite3
from pprint import pprint

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


def limit_tbl_rows(max_rows, db_name, tbl):
    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()
    
    sql = f"SELECT `rowid` FROM `{tbl}` ORDER BY `rowid`"
    db_cur.execute(sql)
    rowids = [row[0] for row in db_cur.fetchall()]
    
    total_rows = len(rowids)
    pprint(total_rows)
    # if total_rows < trigger_size:
    #     return None

    extra_rows = total_rows - max_rows

    unwanted_rowids = []
    for i, rowid in enumerate(rowids):
        if i > extra_rows - 1:
            break
        unwanted_rowids.append(str(rowid))
    in_clause = "','".join(unwanted_rowids)

    sql = f"DELETE FROM `{tbl}` WHERE `rowid` IN ('{in_clause}')"
    pprint(sql)
    # db_cur.execute(sql)
    
    # commit() is only required for SQL statements that modify the database.
    db_conn.commit()
    db_cur.close()
    db_conn.close()

def compact_api_labels(trigger_size, db_name, tbl):
    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()

    sql = f"SELECT `rowid` FROM `{tbl}` ORDER BY `rowid`"
    db_cur.execute(sql)
    rowids = [row[0] for row in db_cur.fetchall()]

    total_rows = len(rowids)
    if total_rows < trigger_size:
        db_cur.close()
        db_conn.close()
        return None

    pprint("VACUUM")
    # db_cur.execute("VACUUM")

    db_conn.commit()
    db_cur.close()
    db_conn.close()


'''
Run code when total rows equal or exceed 900.
Delete x rows to leave 800 rows.
'''
limit_tbl_rows(800, 'api_labels.db3', 'prime_labels')
# limit_tbl_rows(6000, 1000, api_labels_cur, 'prime_labels')

compact_api_labels(1300, 'api_labels.db3', 'prime_labels')


'''
from datetime import datetime

# Get the current date and time
# current_datetime = datetime.now()

# Format the date and time as "yyyymmddhhmm"
datetime = datetime.now().strftime('%Y%m%d%H%M')
print(datetime)
'''
