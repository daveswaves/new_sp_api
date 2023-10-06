# python3 amazon_sp_api_get_orders.py 120 2 mock

'''
https://sp-api-docs.saleweaver.com/quickstart/
https://sp-api-docs.saleweaver.com/pii/
https://www.youtube.com/watch?v=m5T2YQLLyaw&list=PL4844XPLxWVWVkufGjqmsXpn32qAngxLd

$ ssh vova@192.168.0.125
Password: Gardens1

cd /var/www/html/sp_api/

python3 amazon_sp_api_get_orders.py 120 2

$ crontab -e

cron job:
#0 * * * * /usr/bin/python3 /var/www/html/sp_api/amazon_sp_api_get_orders.py 120 2

NOTE:
This script uses the 'title' & 'variations' values from the lookup table:
NEW_API_SYSTEM/sp_api/lookup_variations_titles.db3
This table also has 'asin' values, but these are not used in this script.


Swapping over existing Amazon API to new SP API

✓ + New 'missing_sku_info@api_orders.db3' table hass been added, which should work with new python API. 
+ 'missing_variations' table (api_orders.db3) needs removing.

+ The automated run (40 mins past the hour) for AccessAmazon.php needs to be disabled.
+ The "Import Amazon CSV" link (API Stats) needs changing to 'NEW_API_SYSTEM/amazon_mws/ImportAmazonCSV.php'

✓ + The 'db_path' in 'json/paths_config.json' needs changing to '/mnt/deepthought/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/'
  and 'api_orders.db3' changing to 'api_orders.db3' in this script.

✓ + 'NEW_API_SYSTEM/view_stats.php' needs updating so that it displays skus in the 'missing_sku_info' table.
  $db_api_orders->query($sql); -> $db_api_orders->query($sql);

Info:
Running 'http://192.168.0.24/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/view_stats.php?mock_mtv' displays
any missing sku_info lookup data. The missing data 'variations' is being added to 'api_orders.db3' (amazon_items),
but not 'sp_api/api_orders.db3'.
The missing data can be added (currently) by adding the missing skus to: 'sp_api/query_api_orders/get_missing_sku_info.php'
then running the script. This will add the sku, asin, title, variation (empty or otherwise) to 
'sp_api/lookup_variations_titles.db3'.

'''

import sqlite3
import time
import importlib
import sys
import os
import json

from datetime import datetime, timedelta
from sp_api.api import Products, Catalog, Sales
from sp_api.base import Marketplaces
from sp_api.api import Orders
from sp_api.util import throttle_retry, load_all_pages
from pprint import pprint

sleep_time = 2

scriptname = 'amazon_sp_api_get_orders.py'

if sys.argv[0].startswith("/"):
    server_path = sys.argv[0].replace(scriptname, '')
else:
    server_path = os.path.abspath(sys.argv[0])
    server_path = server_path.replace(scriptname, '')

sp_api_keys = json.load(open(server_path + 'json/sp-api-keys.json', 'r'))
paths_config = json.load(open(server_path + 'json/paths_config.json', 'r'))

# Ternary operator: x = a if True else b
db_path = paths_config['db_path'] if '' == paths_config['mock_path'] else server_path + paths_config['mock_path']

#==============================
# Use dummy data for debugging.
#==============================
mock = False
if 4 == len(sys.argv) and 'mock' == sys.argv[3]:
    mock = json.load(open(server_path + 'ordersData/mock.json', 'r'))
    allOrders = mock['allOrders']
    allOrderItems = mock['allOrderItems']
    
    mock = True
    
client_config = dict(
    refresh_token = sp_api_keys['CLIENT_REFRESH_TOKEN'],
    lwa_app_id = sp_api_keys['LWA_APP_ID'],
    lwa_client_secret = sp_api_keys['LWA_CLIENT_SECRET'],
    aws_secret_key = sp_api_keys['AWS_SECRET_KEY'],
    aws_access_key = sp_api_keys['AWS_ACCESS_KEY'],
    role_arn = sp_api_keys['ROLE_ARN'],
)

@throttle_retry()
@load_all_pages()
def load_all_orders(**kwargs):
    # a generator function to return all pages, obtained by NextToken
    return Orders(credentials=client_config, marketplace=Marketplaces.UK).get_orders(**kwargs)


api_orders_conn = sqlite3.connect(db_path + 'sp_api/api_orders.db3')
api_orders_cur = api_orders_conn.cursor()

# Get existing orderIDs to avoid adding duplicates
api_orders_cur.execute("SELECT orderId FROM amazon_orders")
existingOrderIDs = api_orders_cur.fetchall()

existingOrderIDsLookup = {row[0]: '' for row in existingOrderIDs}

'''
'''

# To date, we haven't managed to find how to retrieve order variations from the new Sp API,
# so this is required to lookup the variation for any given SKU.
skus_titles_vars_conn = sqlite3.connect(db_path + 'sp_api/lookup_variations_titles.db3')
skus_titles_vars_cur = skus_titles_vars_conn.cursor()

skus_titles_vars_cur.execute("SELECT sku,title,variation FROM skus_titles_vars")
# skus_titles_vars_cur.execute("SELECT sku,title,variation FROM skus_titles_vars WHERE sku != 'cabbage collarsx60'")
output = skus_titles_vars_cur.fetchall()
sku_vars_lookup = {row[0]: row[2] for row in output} # equivalent to FETCH_KEY_PAIR
sku_title_lookup = {row[0]: row[1] for row in output}

skus_titles_vars_conn.commit()
skus_titles_vars_cur.close()
skus_titles_vars_conn.close()

timeStart = int(time.time())

# 'utcfromtimestamp' gives GMT, 'fromtimestamp' gives BST
timeStartFmt = datetime.utcfromtimestamp(timeStart).strftime('%Y-%m-%d %H:%M:%S')

missing_sku_info = []
missingTitlesVarsOrderIds = {}

# pprint(mock)
# sys.exit()

# Use dummy data if an ordersData file has been imported
if mock:
    # Remove records from allOrders mock data if already exist in database
    allOrders = [rec for rec in allOrders if rec['orderId'] not in existingOrderIDsLookup]

    # Add variations to order items via the lookup
    for row in allOrderItems:
        row['title'] = ''
        row['variations'] = ''
        if row['sku'] in sku_title_lookup:
            row['title'] = sku_title_lookup[row['sku']]
            row['variations'] = sku_vars_lookup[row['sku']]
        else:
            print("The following SKU does not exist in the 'skus_titles_vars' lookup table:", row['sku'])
            missing_sku_info.append(row['sku'])
            row['title'] = '***SKU_TITLE_NEEDS_ADDING_TO_LOOKUP_TABLE***'
            row['variations'] = '***SKU_VARIATIONS_NEEDS_ADDING_TO_LOOKUP_TABLE***'
            missingTitlesVarsOrderIds[row['orderId']] = 1
        
        
### DEBUG
# pprint(allOrderItems)
# sys.exit()


# Run live SP-API if no dummy data
else:
    minsFrom = int(sys.argv[1])
    minsTo = int(sys.argv[2])
    
    debug_amazon_items = [];
    debug_amazon_orders = [];
    # debug_queue_orderIds = [];
    debug_stats = [];
    
    # print all orders for the last x days/hours/minutes:
    allOrders = []
    for page in load_all_orders(
        RestrictedResources=['buyerInfo', 'shippingAddress'],
        LastUpdatedAfter=(datetime.utcnow() - timedelta(minutes=minsFrom)).isoformat(),
        LastUpdatedBefore=(datetime.utcnow() - timedelta(minutes=minsTo)).isoformat(),
        OrderStatuses=['Unshipped']
    ):
    # for page in load_all_orders(RestrictedResources=['buyerInfo', 'shippingAddress'], LastUpdatedAfter=(datetime.utcnow() - timedelta(minutes=80)).isoformat()):
    # for page in load_all_orders(RestrictedResources=[LastUpdatedAfter=(datetime.utcnow() - timedelta(hours=1)).isoformat()):
        for order in page.payload.get('Orders'):
            # pprint(order)
            
            orderStatus = order.get('OrderStatus')
            orderId = order.get('AmazonOrderId')
            
            # OrderStatus has 4 possible values: Shipped Unshipped Pending Canceled
            # We only want to pull the 'Unshipped' orders.
            if 'Unshipped' == orderStatus and orderId not in existingOrderIDsLookup:
                # orderId = order.get('AmazonOrderId')
                total = order['OrderTotal']['Amount'] # Or order.get('OrderTotal', {}).get('Amount')
                currency = order['OrderTotal']['CurrencyCode']
                date = order.get('PurchaseDate')
                
                if 'BuyerInfo' in order and 'BuyerName' in order['BuyerInfo']:
                    buyer = order['BuyerInfo']['BuyerName']
                else:
                    buyer = ''
                
                if 'Phone' in order['ShippingAddress']:
                    phone = order['ShippingAddress']['Phone']
                else:
                    phone = ''
                
                email = order['BuyerInfo']['BuyerEmail']
                isprime = order.get('IsPrime')
                service = order.get('ShipmentServiceLevelCategory')
                shippingName = order['ShippingAddress']['Name']
                
                if 'AddressLine1' in order['ShippingAddress']:
                    addressLine1 = order['ShippingAddress']['AddressLine1']
                else:
                    addressLine1 = ''
                
                if 'AddressLine2' in order['ShippingAddress']:
                    addressLine2 = order['ShippingAddress']['AddressLine2']
                else:
                    addressLine2 = ''
                
                city = order['ShippingAddress']['City']
                
                if 'County' in order['ShippingAddress']:
                    county = order['ShippingAddress']['County']
                else:
                    county = ''
                
                countryCode = order['ShippingAddress']['CountryCode']
                postcode = order['ShippingAddress']['PostalCode']
                
                orderObj = {
                    'orderId': orderId,
                    'total': total,
                    'currency': currency,
                    'date': date,
                    'buyer': buyer,
                    'phone': phone,
                    'email': email,
                    'isprime': isprime,
                    'service': service,
                    'shippingName': shippingName,
                    'addressLine1': addressLine1,
                    'addressLine2': addressLine2,
                    'city': city,
                    'county': county,
                    'countryCode': countryCode,
                    'postcode': postcode,
                }
                
                allOrders.append(orderObj)
                

    # Get order items
    allOrderItems = []
    burstItems = 30
    for row in allOrders:
        orderId = row['orderId']
        
        order_client = Orders(credentials=client_config, marketplace=Marketplaces.UK)
        order_items = order_client.get_order_items(orderId).payload

        for item in order_items['OrderItems']:
            if 'ShippingPrice' in item:
                shipping = item['ShippingPrice']['Amount']
            else:
                shipping = None
        
            # Use lookup to get variations
            title = ''
            variations = ''
            if item['SellerSKU'] in sku_vars_lookup:
                title = sku_title_lookup[item['SellerSKU']]
                variations = sku_vars_lookup[item['SellerSKU']]
            else:
                print("The following SKU does not exist in the 'skus_titles_vars' lookup table:", item['SellerSKU'])
                missing_sku_info.append(item['SellerSKU'])
                title = '***SKU_TITLE_NEEDS_ADDING_TO_LOOKUP_TABLE***'
                variations = '***SKU_VARIATIONS_NEEDS_ADDING_TO_LOOKUP_TABLE***'
                missingTitlesVarsOrderIds[orderId] = 1
            
            orderItemsObj = {
                'orderId': orderId,
                'itemId': item['ASIN'],
                'orderItemId': item['OrderItemId'],
                'sku': item['SellerSKU'],
                'qty': item['QuantityOrdered'],
                'title': title,
                # 'title': item['Title'],
                'variations': variations,
                'price': item['ItemPrice']['Amount'],
                'shipping': shipping,
            }
            
            allOrderItems.append(orderItemsObj)
            
        if burstItems < 2:
            time.sleep(sleep_time)
        else:
            burstItems = burstItems-1


timefinish = int(time.time())

totalOrders = len(allOrders)
totalOrderItems = len(allOrderItems)

# Assign orderId values in allOrders list to allOrderIds
allOrderIds = [order['orderId'] for order in allOrders]
# Assign orderId values in allOrderItems list to allOrderItemsIds
allOrderItemsIds = [order['orderId'] for order in allOrderItems]

# Find the order IDs in allOrders that are not in allOrderItems
missingOrderIds = list(set(allOrderIds) - set(allOrderItemsIds))
# Assign orderIds to keys in a dict
missingOrderIdsDict = {orderId: None for orderId in missingOrderIds}

incompleteOrders = len(missingOrderIds)
totalOrdersMinusMissing = totalOrders - incompleteOrders

pprint({'Missing Orders in OrderItems': missingOrderIdsDict})

pprint({'Missing titles/vars in lookup table': missingTitlesVarsOrderIds})

# Record orderIDs that don't exist in allOrderItems
for orderId in missingOrderIds:
    api_orders_cur.execute("INSERT INTO missing_orderIds (platform,orderId,timestamp) VALUES (?,?,?)", ('amazon',orderId,timeStart))

date_time = datetime.now().strftime('%Y%m%d%H%M')

for row in allOrders:
    # Don't add order to 'amazon_orders' if it doesn't exist in amazon_items
    if row['orderId'] not in missingOrderIdsDict:
        debug_line = row['orderId']+'<#>'+str(row['total'])+'<#>'+row['currency']+'<#>'+row['date']+'<#>'+row['buyer']+'<#>'+row['phone']+'<#>'+row['email']+'<#>'+str(row['isprime'])+'<#>'+row['service']+'<#>'+row['shippingName']+'<#>'+row['addressLine1']+'<#>'+row['addressLine2']+'<#>'+row['city']+'<#>'+row['county']+'<#>'+row['countryCode']+'<#>'+row['postcode']
        debug_amazon_orders.append(debug_line)
        
        # Insert records into 'amazon_orders' table
        api_orders_cur.execute("INSERT INTO amazon_orders (orderId,total,currency,date,buyer,phone,email,isprime,service,shippingName,addressLine1,addressLine2,city,county,countryCode,postcode,datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (row['orderId'],row['total'],row['currency'],row['date'],row['buyer'],row['phone'],row['email'],row['isprime'],row['service'],row['shippingName'],row['addressLine1'],row['addressLine2'],row['city'],row['county'],row['countryCode'],row['postcode'],date_time))
        
        # if missingTitlesVarsOrderIds.get(row['orderId']) == None:
            # debug_line = 'amazon'+'<#>'+row['orderId']
            # debug_queue_orderIds.append(debug_line)
            
            # Insert records into 'queue_orderIds' table
            # api_orders_cur.execute("INSERT INTO queue_orderIds (platform,orderId) VALUES (?,?)", ('amazon',row['orderId']))
            

debug_line = 'amazon'+'<#>'+str(timeStart)+'<#>'+str(timefinish)+'<#>'+str(totalOrdersMinusMissing)+'<#>'+str(incompleteOrders)
debug_stats.append(debug_line)

# Insert records into 'stats' table
api_orders_cur.execute("INSERT INTO stats (platform,time_start,time_finish,total,incomplete_orders) VALUES (?,?,?,?,?)", ('amazon',timeStart,timefinish,totalOrdersMinusMissing,incompleteOrders))


# Insert record into 'last_request_timestamp' table
api_orders_cur.execute("UPDATE `last_request_timestamp` SET `platform` = ?, `time_start` = ?, `time_finish` = ?", ('amazon',timeStart,timefinish))

for row in allOrderItems:
    debug_line = row['orderId']+'<#>'+row['itemId']+'<#>'+str(row['sku'])+'<#>'+str(row['qty'])+'<#>'+row['title']+'<#>'+row['variations']+'<#>'+str(row['price'])+'<#>'+str(row['shipping'])
    debug_amazon_items.append(debug_line)
    
    # Insert records into 'amazon_items' table - NOW HAS ADDITIONAL 'orderItemId' field
    api_orders_cur.execute("INSERT INTO amazon_items (orderId,itemId,orderItemId,sku,qty,title,variations,price,shipping) VALUES (?,?,?,?,?,?,?,?,?)", (row['orderId'],row['itemId'],row['orderItemId'],row['sku'],row['qty'],row['title'],row['variations'],row['price'],row['shipping']))
    # Insert records into 'amazon_items' table
    # api_orders_cur.execute("INSERT INTO amazon_items (orderId,itemId,sku,qty,title,variations,price,shipping) VALUES (?,?,?,?,?,?,?,?)", (row['orderId'],row['itemId'],row['sku'],row['qty'],row['title'],row['variations'],row['price'],row['shipping']))

for sku in missing_sku_info:
    # Insert records into 'missing_sku_info' table
    api_orders_cur.execute("INSERT INTO missing_sku_info (sku,timestamp) VALUES (?,?)", (sku,timeStart))

# Insert missingTitlesVarsOrderIds items into 'missing_sku_info'
totalMissingSkuVars = len(missing_sku_info)



# START FUNCTIONS
def delete_unwanted_rows(max_rows, db_obj, tbl):
    sql = f"SELECT `rowid` FROM `{tbl}` ORDER BY `rowid`";
    db_obj.execute(sql)
    rowids = [row[0] for row in db_obj.fetchall()]
    
    total_rows = len(rowids)
    extra_rows = total_rows - max_rows

    unwanted_rowids = []
    for i, rowid in enumerate(rowids):
        if i > extra_rows - 1:
            break
        unwanted_rowids.append(str(rowid))
    in_clause = "','".join(unwanted_rowids)

    sql = f"DELETE FROM `{tbl}` WHERE `rowid` IN ('{in_clause}')";
    db_obj.execute(sql)

# END FUNCTIONS

max_rows = 10000
delete_unwanted_rows(max_rows, api_orders_cur, 'amazon_items')
delete_unwanted_rows(max_rows, api_orders_cur, 'amazon_orders')

max_rows = 100
delete_unwanted_rows(max_rows, api_orders_cur, 'missing_orderIds')
delete_unwanted_rows(max_rows, api_orders_cur, 'missing_sku_info')
delete_unwanted_rows(max_rows, api_orders_cur, 'stats')



api_orders_conn.commit()
api_orders_cur.close()
api_orders_conn.close()

timeEnd = int(time.time())
timeEndFmt = datetime.utcfromtimestamp(timeEnd).strftime('%Y-%m-%d %H:%M:%S')

stats = f'| orders: {totalOrders} | orderItems: {totalOrderItems} | missing titles/vars: {totalMissingSkuVars}\n'

resultsTimestamp = f'start: {timeStart} | end: {timeEnd} | runtime: {timeEnd-timeStart} {stats}'
resultsDateTime = f'start: {timeStartFmt} | end: {timeEndFmt} | runtime: {timeEnd-timeStart} {stats}'

# f = open(db_path + 'debug.txt', "a")
# f.write(resultsTimestamp)
# f.close()

print(resultsDateTime)


f = open(db_path + 'sp_api/debug/amazon_items_' + str(timeStart), "w")
f.write('\n'.join(str(item) for item in debug_amazon_items))
f.close()
f = open(db_path + 'sp_api/debug/amazon_orders_' + str(timeStart), "w")
f.write('\n'.join(str(item) for item in debug_amazon_orders))
f.close()
# f = open(db_path + 'sp_api/debug/queue_orderIds_' + str(timeStart), "w")
# f.write('\n'.join(str(item) for item in debug_queue_orderIds))
# f.close()
f = open(db_path + 'sp_api/debug/stats_' + str(timeStart), "w")
f.write('\n'.join(str(item) for item in debug_stats))
f.close()
