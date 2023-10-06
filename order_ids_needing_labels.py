import os
import sys
import json
import sqlite3

from typing import List, Dict
from pprint import pprint

'''
$ ssh vova@192.168.0.125
Password: Gardens1
cd /var/www/html/sp_api/

python3 order_ids_needing_labels.py

Task:
Record ordeIDs that should have labels
'''

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

SCRIPT_NAME = 'order_ids_needing_labels.py'
SERVER_PATH = os.path.abspath(sys.argv[0]).replace(SCRIPT_NAME, '')
PATHS_CONFIG_FILE = 'paths_config.json'
PATHS_CONFIG_FILE_PATH = SERVER_PATH + 'json/' + PATHS_CONFIG_FILE

with open(PATHS_CONFIG_FILE_PATH, 'r', encoding='utf-8') as file:
    paths_config = json.load(file)
    DB_PATH = paths_config['db_path'] if '' == paths_config['mock_path'] else SERVER_PATH + paths_config['mock_path']

DB_PATH = DB_PATH + 'sp_api/'


def label_orders_list() -> List[Dict]:
    api_orders_con = sqlite3.connect(DB_PATH + 'api_orders.db3')
    api_orders_cur = api_orders_con.cursor()
    
    subquery = 'SELECT `datetime` FROM `amazon_orders` ORDER BY `rowid` DESC LIMIT 1'
    api_orders_cur.execute(subquery)
    datetime_val = api_orders_cur.fetchone()[0]

    sql = '''
    SELECT `orderId`
    FROM `amazon_orders`
    WHERE `datetime` = ?
    AND (`service` = 'NextDay' OR `service` = 'SecondDay');
    '''
    orderids = []
    for row in api_orders_cur.execute(sql, (datetime_val,)):
        row_orderid, = row
        orderids.append({row_orderid: datetime_val})

    return orderids



def main() -> None:
    orders = label_orders_list()

    '''
    Converts:
    [{'204-8094616-6313935': '202310050740'},
     {'206-8521019-0534753': '202310050740'},
     {'202-6895255-9693158': '202310050740'}]
    to:
    204-8094616-6313935: 202310050740\n
    206-8521019-0534753: 202310050740\n
    202-6895255-9693158: 202310050740\n
    '''
    orders_str = "\n".join([f"{k}: {v}" for d in orders for k, v in d.items()])

    pprint(orders)

    args = sys.argv[1:]
    if len(args) == 0:
        with open('label_orderIDs.txt', 'a', encoding='utf-8') as file:
            file.write(orders_str + '\n\n')


if __name__ == "__main__":
    main()
