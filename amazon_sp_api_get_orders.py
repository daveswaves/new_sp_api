# python3 amazon_sp_api_get_orders.py 30 2 mock=ordersData60

'''
$ ssh vova@192.168.0.125
Password: Gardens1

cd /var/www/html/sp_api/

cron job:
5 * * * * /usr/bin/python3 /var/www/html/sp_api/amazon_sp_api_get_orders.py 30 2 mock=ordersData60


https://sp-api-docs.saleweaver.com/quickstart/
https://sp-api-docs.saleweaver.com/pii/
https://www.youtube.com/watch?v=m5T2YQLLyaw&list=PL4844XPLxWVWVkufGjqmsXpn32qAngxLd
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

server_path = os.path.abspath(__file__)
server_path = server_path.replace(sys.argv[0], '')

sp_api_keys = json.load(open(server_path + 'json/sp-api-keys.json', 'r'))
db_paths = json.load(open(server_path + 'json/db_paths.json', 'r'))

# Ternary operator: x = a if True else b
db_path = db_paths['db_path'] if '' == db_paths['db_path_mock'] else server_path + db_paths['db_path_mock']

'''
pprint()
sys.exit()
'''

#==============================
# Use dummy data for debugging.
#==============================
mock = False
if 4 == len(sys.argv) and 'mock=' == sys.argv[3][:5]:
    sys.path.insert(1, server_path + '/ordersData')
    
    # from sys.argv[3][5:] import allOrders, allOrderItems
    module_name = sys.argv[3][5:]
    module = importlib.import_module(module_name)
    allOrders = module.allOrders
    allOrderItems = module.allOrderItems
    
    mock = True

# pprint(mock)

# pprint(allOrders)
# pprint(allOrderItems)
# sys.exit()

client_config = dict(
    refresh_token = sp_api_keys['CLIENT_REFRESH_TOKEN'],
    lwa_app_id = sp_api_keys['LWA_APP_ID'],
    lwa_client_secret = sp_api_keys['LWA_CLIENT_SECRET'],
    aws_secret_key = sp_api_keys['AWS_SECRET_KEY'],
    aws_access_key = sp_api_keys['AWS_ACCESS_KEY'],
    role_arn = sp_api_keys['ROLE_ARN'],
)

# sys.exit()

@throttle_retry()
@load_all_pages()
def load_all_orders(**kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    return Orders(credentials=client_config, marketplace=Marketplaces.UK).get_orders(**kwargs)


api_orders_conn = sqlite3.connect(db_path + 'api_orders_NEW.db3')
api_orders_cur = api_orders_conn.cursor()

# Get existing orderIDs to avoid adding duplicates
api_orders_cur.execute("SELECT orderId FROM amazon_orders")
existingOrderIDs = api_orders_cur.fetchall()

existingOrderIDsLookup = {row[0]: '' for row in existingOrderIDs}

'''
if '206-9782558-8601120' in existingOrderIDsLookup:
    pprint('TRUE')
else:
    pprint('FALSE')

pprint(existingOrderIDsLookup)
sys.exit()
'''

# To date, we haven't managed to find how to retrieve order variations from the new Sp API,
# so this is required to lookup the variation for any given SKU.
sku_asin_vars_conn = sqlite3.connect(db_paths['sku_info_path'] + 'sku_info.db3')
# sku_asin_vars_conn = sqlite3.connect('/mnt/deepthought/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/amazon_mws/sku_info.db3')
sku_asin_vars_cur = sku_asin_vars_conn.cursor()

sku_asin_vars_cur.execute("SELECT sku,variation FROM sku_asin_vars")
output = sku_asin_vars_cur.fetchall()
sku_vars_lookup = {row[0]: row[1] for row in output} # equivalent to FETCH_KEY_PAIR

sku_asin_vars_conn.commit()
sku_asin_vars_cur.close()
sku_asin_vars_conn.close()

# pprint(sku_vars_lookup)
# sys.exit()


# Delete lookup record for debugging purposes.
# del sku_vars_lookup['splitcane_600_20']

# pprint(sku_vars_lookup)
# sys.exit()


timestart = int(time.time())

# 'utcfromtimestamp' gives GMT, 'fromtimestamp' gives BST
timestartFmt = datetime.utcfromtimestamp(timestart).strftime('%Y-%m-%d %H:%M:%S')

pprint(timestartFmt)
# sys.exit()


missing_sku_asin_vars = []
missingVarsOrderIds = {}

# pprint(mock)
# sys.exit()

# Use dummy data if an ordersData file has been imported
if mock:
    # Remove records from allOrders mock data if already exist in database
    allOrders = [rec for rec in allOrders if rec['orderId'] not in existingOrderIDsLookup]

    # pprint(existingOrderIDsLookup)
    # pprint(allOrders)
    # sys.exit()
    
    # Add variations to order items via the lookup
    for row in allOrderItems:
        row['variations'] = ''
        if row['sku'] in sku_vars_lookup:
            row['variations'] = sku_vars_lookup[row['sku']]
        else:
            print("The following SKU does not exist in the 'sku_asin_vars' lookup table:", row['sku'])
            missing_sku_asin_vars.append(row['sku'])
            row['variations'] = '***SKU_VARIATIONS_NEEDS_ADDING_TO_LOOKUP_TABLE***'
            missingVarsOrderIds[row['orderId']] = 1

### DEBUG
# pprint(allOrderItems)
# sys.exit()


# Run live SP-API if no dummy data
else:
    # print('Run API')
    # sys.exit()
    minsFrom = int(sys.argv[1])
    minsTo = int(sys.argv[2])
    
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
            orderStatus = order.get('OrderStatus')
            orderId = order.get('AmazonOrderId')
            
            # OrderStatus has 4 possible values: Shipped Unshipped Pending Canceled
            # We only want to pull the 'Unshipped' orders.
            if 'Unshipped' == orderStatus and orderId not in existingOrderIDsLookup:
                # orderId = order.get('AmazonOrderId')
                total = order['OrderTotal']['Amount'] # Or order.get('OrderTotal', {}).get('Amount')
                currency = order['OrderTotal']['CurrencyCode']
                date = order.get('PurchaseDate')
                buyer = order['BuyerInfo']['BuyerName']
                
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
    # missingVarsOrderIds = {}
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
            variations = ''
            if item['SellerSKU'] in sku_vars_lookup:
                variations = sku_vars_lookup[item['SellerSKU']]
            else:
                print("The following SKU does not exist in the 'sku_asin_vars' lookup table:", item['SellerSKU'])
                missing_sku_asin_vars.append(item['SellerSKU'])
                variations = '***SKU_VARIATIONS_NEEDS_ADDING_TO_LOOKUP_TABLE***'
                missingVarsOrderIds[orderId] = 1
        
            orderItemsObj = {
                'orderId': orderId,
                'itemId': item['ASIN'],
                'sku': item['SellerSKU'],
                'qty': item['QuantityOrdered'],
                'title': item['Title'],
                'variations': variations,
                'price': item['ItemPrice']['Amount'],
                'shipping': shipping,
            }
            
            allOrderItems.append(orderItemsObj)
            
        if burstItems < 2:
            time.sleep(1)
        else:
            burstItems = burstItems-1


timefinish = int(time.time())

totalOrders = len(allOrders)

# print('Total Orders:', totalOrders)
# print('Total Items:', len(allOrderItems))

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

pprint(missingVarsOrderIds)

pprint(allOrders)
pprint(allOrderItems)

# Delete existing table data for debugging
# ########################################
# api_orders_cur.execute("DELETE FROM amazon_orders")
# api_orders_cur.execute("DELETE FROM queue_orderIds")
# api_orders_cur.execute("DELETE FROM save_queue_records")
# api_orders_cur.execute("DELETE FROM stats")
# api_orders_cur.execute("DELETE FROM amazon_items")
# api_orders_cur.execute("DELETE FROM missing_orderIds")
# api_orders_cur.execute("DELETE FROM missing_sku_asin_vars")
# api_orders_conn.commit()

# api_orders_cur.execute('VACUUM'); # Compact Database
# api_orders_conn.commit()

# api_orders_cur.close()
# api_orders_conn.close()
# sys.exit()

'''
TODO: Add code for any error data to be inserted into 'request_errors' / 'invalid_orders' tables.
      Nb. It would be useful to record the incomplete order's orderID somewhere.
'''


# Record orderIDs that don't exist in allOrderItems
for orderId in missingOrderIds:
    api_orders_cur.execute("INSERT INTO missing_orderIds (platform,orderId,timestamp) VALUES (?,?,?)", ('amazon',orderId,timestart))

for row in allOrders:
    # Don't add order to 'amazon_orders' if it doesn't exist in amazon_items
    if row['orderId'] not in missingOrderIdsDict:
        # Insert records into 'amazon_orders' table
        api_orders_cur.execute("INSERT INTO amazon_orders (orderId,total,currency,date,buyer,phone,email,isprime,service,shippingName,addressLine1,addressLine2,city,county,countryCode,postcode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (row['orderId'],row['total'],row['currency'],row['date'],row['buyer'],row['phone'],row['email'],row['isprime'],row['service'],row['shippingName'],row['addressLine1'],row['addressLine2'],row['city'],row['county'],row['countryCode'],row['postcode']))
        
        if missingVarsOrderIds.get(row['orderId']) == None:
            # Insert records into 'queue_orderIds' table
            api_orders_cur.execute("INSERT INTO queue_orderIds (platform,orderId) VALUES (?,?)", ('amazon',row['orderId']))
        
            # Insert records into 'save_queue_records' table
            api_orders_cur.execute("INSERT INTO save_queue_records (platform,orderId,timestamp) VALUES (?,?,?)", ('amazon',row['orderId'],timestart))


# Insert records into 'stats' table
api_orders_cur.execute("INSERT INTO stats (platform,time_start,time_finish,total,incomplete_orders) VALUES (?,?,?,?,?)", ('amazon',timestart,timefinish,totalOrdersMinusMissing,incompleteOrders))
# Insert record into 'last_request_timestamp' table
api_orders_cur.execute("UPDATE `last_request_timestamp` SET `platform` = ?, `time_start` = ?, `time_finish` = ?", ('amazon',timestart,timefinish))
# api_orders_cur.execute("INSERT INTO last_request_timestamp (platform,time_start,time_finish) VALUES (?,?,?)", ('amazon',timestart,timefinish))

for row in allOrderItems:
    # Insert records into 'amazon_items' table
    api_orders_cur.execute("INSERT INTO amazon_items (orderId,itemId,sku,qty,title,variations,price,shipping) VALUES (?,?,?,?,?,?,?,?)", (row['orderId'],row['itemId'],row['sku'],row['qty'],row['title'],row['variations'],row['price'],row['shipping']))

for sku in missing_sku_asin_vars:
    # Insert records into 'missing_sku_asin_vars' table
    api_orders_cur.execute("INSERT INTO missing_sku_asin_vars (sku,timestamp) VALUES (?,?)", (sku,timestart))


api_orders_conn.commit()
api_orders_cur.close()
api_orders_conn.close()

# pprint(datetime.now())

timeend = int(time.time())
timeendFmt = datetime.utcfromtimestamp(timeend).strftime('%Y-%m-%d %H:%M:%S')
pprint(timeendFmt)
