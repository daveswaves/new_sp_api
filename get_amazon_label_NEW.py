import sys
import os
import json
import time
import sqlite3

'''
$ ssh vova@192.168.0.125
Password: Gardens1
cd /var/www/html/sp_api/

python3 get_amazon_label_NEW.py
'''

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# print(sys.path)
# print(help('modules'))

# $ pip show python-amazon-sp-api
sys.path.append('/home/david/.local/lib/python3.8/site-packages')

from typing import List, Dict
from pprint import pprint
from sp_api.api import MerchantFulfillment
from sp_api.base import Marketplaces


SCRIPT_NAME = 'get_amazon_label_NEW.py'
SERVER_PATH = os.path.abspath(sys.argv[0]).replace(SCRIPT_NAME, '')
SP_API_KEYS_FILE = 'sp-api-keys.json'
SP_API_KEYS_FILE_PATH = SERVER_PATH + 'json/' + SP_API_KEYS_FILE

PATHS_CONFIG_FILE = 'paths_config.json'
PATHS_CONFIG_FILE_PATH = SERVER_PATH + 'json/' + PATHS_CONFIG_FILE

"""
NOTES:
'NEW_API_SYSTEM/sp_api/api_orders.db3' gets updated by
'/var/www/html/sp_api/amazon_sp_api_get_orders.py'(40 mins past the hour).
The data in this database gets copied accross to 'NEW_API_SYSTEM/api_orders.db3'
by 'NEW_API_SYSTEM/sp_api/pull_amazon_orders.php' (on the hour).
2 fields are not copied: `datetime`/'amazon_orders', `orderItemId`/'amazon_items'.
These fields are required by this script.
This script needs to run after 'NEW_API_SYSTEM/sp_api/api_orders.db3' has been updated,
but before it's copied over to 'NEW_API_SYSTEM/api_orders.db3'.
This script has a 1 second delay between each API call, so 50 or so orders will take approx 1 minute.
It runs 52 mins past the hour.
"""

# Set CONSTANTS
TEST_DATETIMES = [
    # '026-0093288-3317118',
    # '026-2028025-3292341',
]

TEST_ORDER_IDS = [
]

REQUESTS_PER_SECOND = 1

ELIXIR_DETAILS = {
    "Name": "Kevin McGuinness",
    "AddressLine1": "Elixir Garden Supplies ltd",
    "AddressLine2": "Unit 1, Middlegate, Whitelund",
    "City": "Morecambe",
    "StateOrProvinceCode": "Lancashire",
    "PostalCode": "LA3 3BN",
    "CountryCode": "GB",
    "Phone": "01524741229",
    "Email": "info@elixirgardens.co.uk"
}

DIM_UNIT = "centimeters"
WGT_UNIT = "g"

'''
`amazon_orders` that have multiple items (2 or more duplicate `orderId` values in `amazon_items`)
orderId, datetime values in `amazon_orders` whose `isprime` = '1' and 
(`service` = 'NextDay' OR `service` = 'SecondDay'):

203-4765016-6670750|202309261045
204-0702523-5682737|202309260245
204-6136111-2833968|202309130832
204-7239200-3941920|202309261745
204-8279938-1253162|202309130832
205-4911023-1463510|202309251846
206-0716898-5257164|202309130832
206-3371165-7575569|202309260946
206-5983540-0330759|202309251846
206-7157310-0961907|202309270145
'''

try:
    with open(SP_API_KEYS_FILE_PATH, 'r', encoding='utf-8') as file:
        sp_api_keys = json.load(file)
except FileNotFoundError:
    print(f"The file '{SP_API_KEYS_FILE}' does not exist.")
except json.JSONDecodeError as json_error:
    print(f"JSON decoding error: {json_error}")
except Exception as e:
    print(f"An error occurred: {e}")

client_config = dict(
    refresh_token = sp_api_keys['CLIENT_REFRESH_TOKEN'],
    lwa_app_id = sp_api_keys['LWA_APP_ID'],
    lwa_client_secret = sp_api_keys['LWA_CLIENT_SECRET'],
    aws_secret_key = sp_api_keys['AWS_SECRET_KEY'],
    aws_access_key = sp_api_keys['AWS_ACCESS_KEY'],
    role_arn = sp_api_keys['ROLE_ARN'],
)

with open(PATHS_CONFIG_FILE_PATH, 'r', encoding='utf-8') as file:
    paths_config = json.load(file)
    DB_PATH = paths_config['db_path'] if '' == paths_config['mock_path'] else SERVER_PATH + paths_config['mock_path']

# PRODUCTS_PATH = DB_PATH.replace('FespMVC/NEW_API_SYSTEM/', '')
DB_PATH = DB_PATH + 'sp_api/'
PRODUCTS_PATH = DB_PATH

# DEBUG
# pprint(client_config)
# pprint(PRODUCTS_PATH)
# sys.exit()


# PRODUCTS_PATH = ''
# DB_PATH = '/mnt/deepthought/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/'

'''
Connect to 'api_orders.db3' (the one that the Python API updates)
and get list of `orderId`s from `amazon_orders` where records all have
the same `datetime` value as the last record, and `isprime` is True
and `service` equals 'NextDay' or 'SecondDay'.
Use the list of `orderId`s to retrive records from
`amazon_items` (`orderId`, `orderitemId`, `sku`, `qty`).

If a datetime_val string is passed to function, eg. orders = label_orders_list('202309261346')
The `amazon_orders` records with this `datetime` value will be used.
'''
def label_orders_list(datetime_val: str = None) -> List[Dict]:
    api_orders_con = sqlite3.connect(DB_PATH + 'api_orders.db3')
    api_orders_cur = api_orders_con.cursor()
    
    if not datetime_val:
        subquery = 'SELECT `datetime` FROM `amazon_orders` ORDER BY `rowid` DESC LIMIT 1'
        api_orders_cur.execute(subquery)
        datetime_val = api_orders_cur.fetchone()[0]

    # pprint()

    if len(TEST_ORDER_IDS) > 0:
        sql = f'''
        SELECT `orderId`, `orderitemId`, `sku`, `qty`
        FROM `amazon_items`
        WHERE `orderId` IN ({','.join(['?'] * len(TEST_ORDER_IDS))});
        '''
        orderid_orderitemid_sku_qty_list = []
        for row in api_orders_cur.execute(sql, TEST_ORDER_IDS):
            row_orderid, row_orderitemid, row_sku, row_qty = row
            orderid_orderitemid_sku_qty_list.append({
                'orderId': row_orderid,
                'orderitemId': row_orderitemid,
                'sku': row_sku,
                'qty': row_qty
            })
    else:
        sql = '''
        WITH LatestOrder AS (
            SELECT `orderId`
            FROM `amazon_orders`
            WHERE `datetime` = ?
            AND (`service` = 'NextDay' OR `service` = 'SecondDay')
        )
        SELECT `orderId`, `orderitemId`, `sku`, `qty`
        FROM `amazon_items`
        WHERE `orderId` IN (SELECT `orderId` FROM LatestOrder);
        '''
        orderid_orderitemid_sku_qty_list = []
        for row in api_orders_cur.execute(sql, (datetime_val,)):
            row_orderid, row_orderitemid, row_sku, row_qty = row
            orderid_orderitemid_sku_qty_list.append({
                'orderId': row_orderid,
                'orderitemId': row_orderitemid,
                'sku': row_sku,
                'qty': row_qty
            })

    return orderid_orderitemid_sku_qty_list


'''
Looks up the 'sku' in the 'products@products.db3' and returns 'weight' & 'length'
'''
def products_lkup(sku_values: List[Dict]) -> List[Dict]:
    products_con = sqlite3.connect(PRODUCTS_PATH + 'products.db3')
    products_cur = products_con.cursor()
    
    # Create placeholder (?, ?, ?, etc)
    sku_qs = ', '.join(['?'] * len(sku_values))
    sql = f"SELECT `sku`,`weight`,`length` FROM `products` WHERE `sku` IN ({sku_qs})"
    products = products_cur.execute(sql, tuple(sku_values)).fetchall()
    products = {item['sku']: {'weight': item['weight'], 'length': item['length']} for item in [{'sku': sku, 'weight': weight, 'length': length} for sku, weight, length in products]}
    
    return products

'''
Groups order items for any given order and returns the following format:
-----------------------------------------------------------------------
{'items': [{'orderitemId': '31794332182002', 'qty': 1, 'sku': 'p_square-pot_07cm-x-0040'},
           {'orderitemId': '31794332182042', 'qty': 1, 'sku': 'p_square-pot_09cm-x-0040'}],
 'orderId': '203-4765016-6670750'},
{'items': [{'orderitemId': '31795147184682', 'qty': 1, 'sku': 'p_WNY-VFB-LCR'}],
 'orderId': '204-7443150-0331545'},
'''
def orders_grouped_items(orders: List[Dict]) -> List[Dict]:
    grouped_dict = {}
    for order in orders:
        order_id = order['orderId']
        item_dict = {key: value for key, value in order.items() if key != 'orderId'}

        if order_id in grouped_dict:
            grouped_dict[order_id]['items'].append(item_dict)
        else:
            grouped_dict[order_id] = {'orderId': order_id, 'items': [item_dict]}

    return list(grouped_dict.values())


def getbox(weight: float, length: float) -> List[Dict]:
    boxes = {
        "Letter": {
            "Length": "35.3",
            "Width": "25.0",
            "Height": "2.5"
        },
        "Parcel2": {
            "Length": "45.0",
            "Width": "35.0",
            "Height": "16.0"
        },
        "Parcel7": {
            "Length": "50.0",
            "Width": "40.0",
            "Height": "30.0"
        },
        "Parcel15": {
            "Length": "61.0",
            "Width": "46.0",
            "Height": "46.0"
        },
        "Parcel20": {
            "Length": "67.0",
            "Width": "51.0",
            "Height": "51.0"
        },
        "Parcel23": {
            "Length": "120.0",
            "Width": "60.0",
            "Height": "60.0"
        }
    }
    
    if weight > 23 or length > 1.5:
        return False
    weight_ = 0.1

    if weight > weight_:
        weight_ = weight
    
    if length > 1.3 or weight_ > 20:
        return boxes['Parcel23']
    elif length > 1.1 or weight_ > 15:
        return boxes['Parcel20']
    elif length > 0.7 or weight_ > 7:
        return boxes['Parcel15']
    elif length > 0.35 or weight_ > 2:
        return boxes['Parcel7']
    elif length > 0.25 or weight_ > 0.75:
        return boxes['Parcel2']
    else:
        return boxes['Letter']


'''
As well as the orders list, the products list (sku, weight, length)
also gets passed to this function.
It uses the getbox() method to calculate the total
dimensions & weight for the order being shipped:
* Height
* Length
* Width
* weight
-----------------------------------------------------------------------
{'getbox': {'Height': '16.0', 'Length': '45.0', 'Width': '35.0'},
 'items_data': [{'OrderItemId': '31794332182002', 'Quantity': 1},
                {'OrderItemId': '31794332182042', 'Quantity': 1}],
 'length': 0.26,
 'orderId': '203-4765016-6670750',
 'weight': 0.528},
{'getbox': {'Height': '30.0', 'Length': '50.0', 'Width': '40.0'},
 'items_data': [{'OrderItemId': '31795147184682', 'Quantity': 1}],
 'length': 0.36,
 'orderId': '204-7443150-0331545',
 'weight': 2.2},
'''
def get_item_properties(orders: List[Dict], products: List[Dict]) -> List[Dict]:
    order_data = []
    for order in orders:
        items_data = []
        
        weight = 0
        length = 0
        for item in order['items']:
            if item['sku'] not in products:
                api_labels_conn = sqlite3.connect(DB_PATH + 'api_labels.db3')
                api_labels_cur = api_labels_conn.cursor()
                api_labels_cur.execute("INSERT INTO `prime_labels_missing` (`orderID`, `label_data`) VALUES (?,?)", (order['orderId'], item['sku']))
                api_labels_conn.commit()
                api_labels_cur.close()
                api_labels_conn.close()
                
                # with open('missing_skus.txt', 'a', encoding='utf-8') as file:
                #     file.write(item['sku'] + '\n')
                
                pprint(f"{item['sku']} does not exist!")
                break
            
            prod_weight = products[item['sku']]['weight']
            prod_length = products[item['sku']]['length']

            items_data.append({
                'OrderItemId': item['orderitemId'],
                'Quantity': item['qty']
            })

            weight += prod_weight * item['qty']
            # weight += prod_weight * item['qty'] * 1000
            if prod_length > length:
                length = prod_length

        if len(items_data) > 0:
            order_data.append({
                'orderId': order['orderId'],
                'items_data': items_data,
                'weight': weight,
                'length': length,
                'getbox': getbox(weight, length),
            })

    return order_data


'''
Converts the order to the format that's required
by the Amazon create_shipment() method.
Nb. The 'ShipFromAddress', 'PackageDimensions.Unit' and 'Weight.Unit'
    values are saved as constants at the top of the script: 
    * ELIXIR_DETAILS
    * DIM_UNIT
    * WGT_UNIT
'''
def shipment_request_details(order: List[Dict]) -> List[Dict]:
    # pprint(order)
    # print('-----------------------------------------------')
    if order['items_data']:
        return {
            "AmazonOrderId": order['orderId'],
            "ItemList": order['items_data'],
            "ShipFromAddress": ELIXIR_DETAILS,
            "PackageDimensions": {
                "Length": order['getbox']['Length'],
                "Width": order['getbox']['Width'],
                "Height": order['getbox']['Height'],
                "Unit": DIM_UNIT
            },
            "Weight": {
                "Value": int(order['weight'] * 1000),
                "Unit": WGT_UNIT
            },
            "ShippingServiceOptions": {
                "DeliveryExperience": "DeliveryConfirmationWithoutSignature",
                "CarrierWillPickUp": True
            }
        }
    return False


'''
Makes an Amazon API request to create a shipment.
The return data includes the 'AmazonOrderId' and the
label 'Contents' (the data required to create a PNG label).
This script only requires theses 2 values.
'''
def request_label(order: List[Dict]) -> List[Dict]:
    return MerchantFulfillment(credentials=client_config, marketplace=Marketplaces.UK).create_shipment(
        shipment_request_details=order,
        shipping_service_id="prime-premium-uk-mfn"
    )


def mock_request_label(order: List[Dict]) -> List[Dict]:
    return {'payload': {'AmazonOrderId': order['AmazonOrderId'],
            'Label': {'FileContents': {'Contents': 'H4sIAAAAAAAAALV6ZVBc0bZmExIgePDg7u5OILg7jRPcIbhLoIGG4O5BGm2kcSe4k9C4S3ANLkEm9829983UvPnzpubUd2qfteWsferU+vb+Vm2wuqocBioxKgAAwFCQl9YEAF7XAgDIKAiIf2sI8ire/i0oLeQ1VQAAP3oAICQcAHj4WxWyDwB4cgIAh2YAgHAGAEDomtOmIfq34bWNpIokAACLC/ijkfzXRnDXlJMCVE+SHvw1kNSVtWX+li8vL5DF/NW/T29d5fXdAQB26n/cCBVbq0l/3QEUpCW1fZZOcpfjVxYyF7r7CtvtHG2yvNClY1JnU+ZrEJZ6xBnB0F0fjP4P5B3BkvA11gTRuNjXU6yW1+X6CEWir3XztgCSTZcpojuvtnuwbjCVMixTADEnAISkN45YSpUyxGi0fXNeBFCkGIa9QcNET44kuaJ4Tb36BdYLvfX7SW8hUcLe7/0Tr/qlWP8rQG8E/Nvj4zsxJIZrsb1No45jPgcdhw0dMixXd5GIBv98PO+B78IuQNFOdXUNGVpP32xkqUqsD3W/MHdjzrHRwLYq4s4igVEurHMOLUh3wG5fJGzVMt6gGr/eNh+EZFzvK7krrNDA5CJSIQrl4O83T4Q/NuDId8wnOgYIr3l3auApXM6WYURSm+dh7BjBbLUFDDTU3ihALtd3U4EmSaQcx/o7yCsbLBHDuE0SiQQTBwrNcWdMeNZaLTXxT2mcZgDTeMp7Rcu40G8I93j7qAzs9Ty4es/E5208K0QskwHNWo+r2bhmaiB6VaLu9f5M/sgNPfIOn4kfV9+7AbKdExA6xa/i73S+4CnyZVRen/ei9nDsjrl+HnXawpA2JDSXbTFTmhG6gBbrG+xRF3y7WEkJDvOxgI6PoilwQQx/EBrTGXlweIZ+YBCdUgxddbNbhbqmfkiH8Oy9KqdU6/CR49NwTI95gTp7cugHIL2tayNbUxUYFH5XsSXy/W52sEGQdz6hTDN+xRfV1nfr1xRV8P1SGhy+fnjPzGee+dMJk/TPxkXNHfsyplLw48hhk2/r8lWYGdpqln2ywPnl4KvdXbkjxuLa1uvY8Ew5jE42na5i+cmtlFjWzgGR+ugWYyq/08MJDmlq5GcpNiOaBMd+t+9s888gP54iFovDE2oDqPmTW4RkQHdGd8vl8qtRwm94HeuRY22Q5fimXhzf15tCoEmGFZsuR6NN3ZGgHX45bI8SwRdW4XwLg6HlQUOvcsXErcEw18xoBT3laTLCQqfysxrMtrxkKt9H9fpBznZNltZvaw6OIV9X2FDPP2SJKjZQLxfeYGn9DggaAjU6Fq9KBchP8jc0xGZigYVGNxR1q3CfAyku0d7UKZMjBqCOLZk90QStW36XeSRGkiji0+K8VjMziTEnGQk6tPykrfGGOGZ5PVXI2CHGRtRu4v1PoYhqMmhxj11guT756N6f7v3igUG2TaaxhC4eAbcInWUgdBVvrc/vA++Qgm9uWUfXRDIdVa/Zwa8Ufu08YOEjihZ/o0J5mLqcvDMDUOPaoHqcehdVB+gIh4QOU6NsYmb1vhEQEbzTa0+aY7xk1wuapliNHtqAqbgLZxxeqs4Ptl00EyzYGHGK+JDXTUtdIe41vlFdejWnPeZX/1MvpWiCOLsZp4bDDjy2+BiZpRNlktFbBWFaWkLa4gRmb8v3876Rd/Nr2+76uARpmPEsvIvqBVHwtk4unHVg6zNlmXUJdCcgBhuzmg7SETJf5MHHm4cFw5Gn5zkLLiecDO4KPT7/yuqWuvzwbkwN66aG8emUYdq0YxcZ6/lP/Vkn6cvd+uPNWmzUzuaiXJ+EaGvAzZMQ+FpO2MfbnKgXi6RoYBi/NUj6x0ZRYzH+0EVlSYVyr2AlH3fi5tp86R5a/+/8i45jcGp4Y1KdjkW8/wDH3frcbBVnTKgWCk4VD+sPSoJZzRcptIGd/NyHWsrw3+YOZNBO6dW7NHTfSNcj1U/W4SjriG3D80KZspzcL+yTin46PQ+FqCJlzBhHn8snlPk1NBZK0aS0BX4Et+ZNRchyBqPDEVtYrvmZla6igtXxP1jqffQUMHkMH5+1ouWbZlBJy7Wf34ZeQSfsoymR9ZsUmJvsgixdv002UwqhffnVibRDUjYmVIs85ECEcg56xFBhqoHmryAuxFN1bgmI5VTB5KhWujGoZ+MxQx9HToz1763Y5AG33O6KhCaSDWPrERMwep2Rka1sR4qa4wWG6pVlymJj89yagM6NQhXok33Vn2+bC6TNJ39yFWKqVtbhR5rNcE8/kp/8sDyBBoLAWsV6nEVYg9cFG5aZCwOQxW7i0O6CUtRep9l+SkdZF95X1KQtPnfB1Ni+cODHvUw1p6ObU4d4giURMn86XZfq/Wmp9BKWlaKqzZG/FGim45jskh4PTFOEtC27vSVtUY9+FLUBqK7lyd7p94y09MWVSzP0T3d3aLxA9ZSKSoXEvt+KOMkf8q/9B7U71yIlagANH1oPLsyuHFXZ49oa3fOj4P/1QvD/GdzEMfgs4bFlwFy78f6VvoHw7T6TWudDHsdKpsShBjW7CWOhx455qa8VRzGG2Ic6CIwVAWQsAdrGzzRz5uZiJFG+SSTqrP3CusaM1WBg5vt6/2rw7czhdU1xVuWIf34okGxD9tjkJlzm1U0lp99Il+ImhAPzEa9EAuAjBqHw0zO9fpFRAyKkR3N8tQUU8N/Njwr/jIr5Cjep7VCvvsCmNU/9JV91jK0AsnjE4jpSag4/e6sU7WLjly/lOQJXha0QUcM6tANYNGNYNdi/a4V4I12Rsbd6dOdLedx1dtN4dWH+Pjn+Vgb2kvLjxb7RTpsN6/0Koqv3IxJ8Mjvg43vKpKwlDDPthccgEB4l6sPei3OJ4lwIs5bfgBGAQTBRDNt6gMYL0KR47vvMZPzbn5fPsJbNJPsd/JvUqWcYaUZamOX6G3zhq69WDODatweSJnxU4qdkR6OaeAoj0jJBZ0DCLOobxcUEhDYdbjykpoUUZXw6JLOpTNY+w44Fh+xs4wKUYKOCQlm8LYoFE7p3LI7A4qSXoFpxpLvmrdsXRrI/bEyac55jKkK2u9Ymr4OcFfaePkR8qfHVN0wOKt6xrND1lO/k1U4Js3Y1ltxcMIgJeJVX2ZGA+MGvAkyQI3yaf/lxkra5svq2G6ILbqyJJLOObpHCMKci+G9hDrO2WvRc+DwtSP4r0hnm0SLL+cMWvtGO2K6+1stI+YvHDXb992rF5+h9yZNklQi2VD/Nw8+Ez/AftIsLaaNpTbRV2l5Y1mMib00ruEj4wI1hqcOqcwcXa/ufcnMaSg1hHZ9eXUaccqwBZaa8nCl194PIG9Y7u0VTv3cRGaCG7TSL2hPoXrOcuvB4lMOXDQ1Ylw35nb/68Trq/SKBdtQH0BaeffQGoVQDDu2iVt3yW6bMNLNOZ/IM8N3xaoDvO8mLGyNAh3hHm3AcB3H7ZhDdDDNBX1vkYzfydiDFILnRomiJD/fNXSttZZjeqRbl2szuOcK5m3YQPfZD0sGoMj3710c2mGoxl5sO5yaPH1PjseiXmPKOk5dPHhWyBRVaGcSxo4o83L95SnsRLZd83390hWVLKWtzfqbxamn89jiaAx6RMDoiIQc9ppDgr++bglGjbTW9yvfAd49NQA0CAeMYh9LM6RJSVnxqlG+kdgMJBheuBtHwLy3c5i4CbQNKPz/RpbOtWPS+NtF0JZSqCuJP9OStWCJsQoTWXuHTBA74q6O3RNTWZhoxu9lmJueWSRPrsrm1bVKOc8/130rjkZUxbqmTPRJpurFDaO33cy9sglWehFkPud5SBSSsUCUS7AU+b+Y7mkZ1CbpSFUaZn4bgsTYe3fFdqoLwVnsv0mv4G5tQxqD6Igr9uaRPu4zd7ArWXyxwIheJtPUPJUZSko+p3zXavxNSiQ5ZzRbiYNReyIo9JvtJP7DWmJp+ezWdKZX64CRg6ZZ3F27oviax1nr5znTZATqdamujuBLxWy5bg0tsJA72qpychFjS6mNHbn+iFTTwHpTreYjFRthiCJ67mTZWYGEsfYnFnYi1xpJeBGvKa+kR/TEmLGQN6fE7qNSbZbV1RlmQJJJPtx+izFkf3jq2zKnn+1X+SYVBNiZeNsuZ8L132MnlRUaFeNXWyLjYk7uvObvsO5nwE7ytCJIJUZkGd1YqBpfO+8BH/ROsrVXz2G2roaNAcUeGSMfRiuSq6gbcvsYPzREqfLnaTu0C7lupaJNxkB2myA5tC4UJLm2j1twmKjbvelSg9JquUptOo9VB9GHeiKtAPZpuSY6Wu3f/NerOODDMtctJfF1IVzQZMKlz/yFMS2M7L089Qd52vnR2m3OVYKzzXdpM5qFsWJS1TUr1Y3l/5++9kNJE8xKV+5MSONwUrifjBVl7QgJCbVd99J+nbRT96BDHfaZe/hIHM43D2wn2Zj3fmp+3UFlU2vUqoCsby22jeOIHuyScs1Sdzq5murRkMlfPaOh1Bt7DWYWIjU7IDMd2q4gvXDPzlcxwQUhtlvY6ezCG0m+hAbC0fYjY/I1zuBUTQEwyv4pDjey5q3ZHSrV5gwXCU4c7i6c0kwdWK6+nIyhAPD/SLj8MWTfu8uwZ674gJ1VKmH5EZ5HQ+JCLeRf6SzHgyIV3Xd/XOq3yDLVcOdj4/Zj0Wyp8NtL7QPP71XwJxvzEFMXY7NoW5ryZJYKxbxgipJT/PZb6X8DWS/znIw9Eh7jgp6BqCwOrfUkDKw2MaK6frxYOB8bTRMMNvqYKlhk6mLW6YnBCCLfSiWRGylwAuui/0x/Pzx6fXb5LQCzsX3B97ot+RknhXQLCg9NeiaVraee9yJiCgOSNhL5M0uYeZLhhlUKkCuPqvrGJGF5kX96CiB5csfueJAFnUZJ9CECaj+j96QhdA2PXSgHeG1zZ779YlQhNWGdEBTni6Cb8yiU+CMBJRuXKd+9RVOm9kBK763wRbIRhK5yNyFp5LQSIYzGn5tOWZEzTWFEH7HiUzLyG9jpRsLRqjKHY7VxeCxrYNlGBhnPqVl4NvWZ18DqTS3wHH/0q4q93ykU4zihtbCuQOcT/KmZbXA4ZotdFxfwSnDoYXOL53PoOONOlO3BH7HcTey3EtCMp1zq4FlDEdzmpI1qBqcljXyhRnFdXdDgf3/uNASrjHlvbHvzY/IrG3tzqiEs2EDffXBqWCNSrjge1MKdZRQvlqoREJbbhd00TEqiuXt33EuPM727+waVP1yJu5Wwyk440t6L3o3u2jgffhudLGsHg8IQ9OT8QSKPVaiEv7RLyDsRS42Mt7CmQottU/ZXi4zD3uo9J7Kj5k3+WzLznmyrRTozMeieqksVYnNYUKSGqutn86eOQXFZ5KeYC5FXV5jsVUyv/ciNFSbplDT4nJUhHf6VX1iJNd93XCHdsgWRdIGbBx9x0cLsEe9brhawTkCUJdCgsakdKDZMJnZtqFVkw6Q+D5uoVzc9+MLnlKyOoiAcDknn+RNl5s7bC7JnYy8GYBmRDvoOMYOLNqRvoc2GJC7mv+Gzp9nKvX3ePomGj2raSMl6BLpBQkdaTVYLBeSqnYiLbe1cBogu5mMGvzOhk01XvS2juXifSvMxf9pvH1fqjauNUV8PsWIFlbIS2s1dyz3cpoH5vAHudxG+n/m0lp9YUNMWyrqX0Ju5l8P4ue6bzgei5fts9ow8L5iKdbMP55HS3IdftDyPXXSD1nr1FvM/h+PNxv9SI91PQGgTKolHRpV6Iufp9HVyzmRUTazJIIl+521Mo9M1Zm/vpU2uSrR6wDkjFEbKX2EFgKO5QNwtwddcS2x89253ipm62R5iIEc4wVmZkX55a0IlcCTMKTlbxiBmxhB6/yW4OcPZkJlwQilCvCRpw6A2RlQQacJ7xS798B0Vidw11L+V6GjqithoTeqvJ6Alq0Mg1aBNG7OgP04h8IeTRjbBVe+f7jYEPs7X82yZqeUzjh7vWxU/ujiU6Blc/XFoqRNzlmUMQWZRjXfha/OjuGzwwczGQZPMY7evG85BulFVKsjO1ylmbCMe/Iw3oqY0KfPskwCRLrt2zqX+gFAlC9jG6WQpqSMIrdIJp421N7oMIRz4MROIKz4ciXX4mXXijaxy52nLw2+48Ugs2o7M66ZPtd6gPtSY8nImWko9FAm0alD3cN7t5U1bVgeQPZd6l7NQavPlzMAi/IMjBS5vE3vkVMWBARJUjg3FCIjkQ3bngVBfMp0afvGlf1QXWfeNCAjUAfNY4/XKSxntaVxGgJBCsxdp8IGtSxFEzE1oG85SU+DSW3zG1ik9thz6MPPVwE9jLP8dmU1LhizEoFTXgRWtjTXjcMheloWZaNBFYyI5jatSv2NKK1Q8SrxhlQx/dNDbuydW3Mu0emMoy/Cz/ecDVxO26PNWUathE//potkKzAyGt4yAlcukmX6t84MfAKBaHE1G7vijNkpWlpL7CnFbUWZtEugiHYgx+C6uWl4rAeWd+NChJwGPWrTFphnr2TRZUqj1qLuWiAjP9VNnzpuf2A0O4AYRdsOnWEdFOf1GGFb9lmERkJ0vdhHqNvQusxa1q4j6zd3hxKaDjS/28bN6qDct5ve4+VorKKQcVyTYQUrFXItLTNTE1FOE4coX0Kso9BzamPkGRWDzbGFJu01qgAYqijMtvNXEwQn9tCYc2cTRTOvxotdLRbYrTcsXcMIPwtwwOvdEOLthcRNXz6Twsikv3zhNdGh21MReHZJt80k/u8SI0hhSUpZJzoMFWclFDe5JXzarwagI6LjqH44obNZ03PEyi5pjTzjp1XHaqzjEIzN+1Kb1tce0oymqZPsnlI6/bmNb7/6u0ejEEVrw6NovJo0DA/yuuCT7FVtG9aBj8055dnjsJa4chy5nbUZ/74VnLZdJhJmk/8vVLSf8zT9jcx7CmMiB8AX9i+gEEKcCw36HBsP+isnni9MbrK2ngqHEBsC6hNPwXwi8TzTu5A4wh8h7Rd//RBw22RpA/zCOtxFBqwcIG/7jJUkUTAYr652SenAIbcifFxSxBjpxq/3ZHp5ay6lhMQSvKsl1e+PtrflonFQH1P1fcTjJxjdZeBNw8jeLIbcbZL1PP7y6l//n5fkoB1IlOpeUBtYIJavitwdoi3gj5DNT/1BaGqQyjeplUqjviCJvUAwCM739niD6YdYHFLaf7Xuo+vibIsVMp1S3eWD16NbPBhzZzdJ5J9JcEx90PkJ8/Aq5gJMmGT4eJccx6LsEW3+/sj6+4XhS2Xe5vss6MLoIJL4rRUET6FbfJ7ifotMfpvEeCx5qZpAMEtv2xo140huBbEHfMx+ovHy2Ry/1YRtnxpd2KXtV8ZBXeqZCBPO9TlKxxx6ag0LMtYEJS5SHILRPSGMXf1g9RJc8OyGoo8okeH1hQnndRvohwvDFw5S3Z7nEPcXnRu77ND/7ErxYOH5bmov4orin6kHJMqQZlRB8qQFjRcWnx2G9/vEWGB3yXtXYgfBf+wp7cNBIsMLIGp5oNVhb4s5V9pAsw8ncmbNQMWWHH15OZF9a+53hdhwqHF2xVaeLaTJM4hg5ICJfZL1AmW+jqrVZD+NY9pmNzNqqLd3RmoGvmM3qspjffbTgrkAgP6W6e9yg/5SqO5ValGyzL/JapwC/4sT9jk7nBWAwtDrFLB35SmaIiTFwKdgzVDKrUWs0C65Tz6zD3U8GTIxsBXyLhhaWVthypYdzhjLO4sp8THPMZHvBxz+ueb0sfSES8ONpkeMISmIso6ayYcNUay88wK1v0KZ27SvUvpC59jDGyDh0rgXRoDwdf5e/oLAx7E7I2T6qxn9KF0aJTccCmiXcUBxcYIgMV1pgdijKqrVF9j+GqPBQGnnGgmHceBT8rpb0F4/2ZpFtXEUZePRDOi+aau46z81DYE05qtEg+4/ik3m398mga9WKuQtRk0lKQC/y8o124bFcK1F9E13TiKkN9ueOg70QPo40T6Og7XVJl+Yxv1Wur4ougR0PK9foj5rhNm/cE8MeN7AwgtgpiNb9Eoy2JWpqb5NRd1Ig49eEULcY0cOAi1QnIVlHccjhzSsRSlWZ2Wl61gpGJzZtuvHWzfEP4vi6L3+5M0ExP7qqPHZKa5IEZ4wVuBamJsElXEgSnGpnH88F46eNQK9wiODqLmA0rl8Oh1IyooNI5dq5S68BZ6SCLl3jBMfnyrju+7fRA6Msjlsg4rhJXCofygcO3vg7p1JC8fWuITZhi3rAx1Q9kyB+LJR77kdFmw12wF8RzMJukM2mi654AJF8f9cPeWbCussdTDtgw4/FrrubXUe3MGX8gG50Q8mOpruaQsNN1m7Kp0uI4efeZiTFtnK4goWp7JTiSK+dti+QryyuAvCv1EUXwWEPV0kun3CPdlz6EkyFnsQ0Jhuu++MOm4vjv1qUk5oGYtW0U+YzZykPeOx9kTjEycxCN2nrPjf+TQzqdVmtiCrglZ6fpX4utl68oxliZUf0r6Pck1MY7qH5gy9BUkFf66D1y8ClZ/LvRMEQmY/jN+/PtZ3n0B2qGcmKIvGjKv7b2143XM16j3tGw6E9TKdH/9uaSrqatZPFhRMK66O7c91+sdN3kKbFe7vHFntiV6+x2VRtLybI25hxcZvgvzh3cBscIjOP+kziP2M7BwKj/k5tLLEb2wNO54jor/NeiRX97zNaNnvksxzffX83G1/4H7R5V9QgbjpmZJJasntSiWCiPv57QPCe+yAcgeQSTQW02eoYj3mq0a8GsN6KwAx1g3zas4ag7Q2F1Q4WVWlzMc13ZqxMvbb5Ua0k1QwlVKL3r+dVxdOlvr6JXl0wFBjSfHynXb+cMYLQEMczR63/V3bOnFqvDrnZw1TXJjBuZHcxwkZ1/iBoyPzGnXgDyqDQYeWRDf13VaUyjiosvq50+POqdXj5n4BXj3HQjqz7T8u1maCwWNtilXpLILiKXsptSfekpj16gdWx9w7kKH+3+uV6pHnZ06dnf2Jyut+/oTTjJwNSZxjJ/aMURDHIg2pxF7cu+Vyaqqn8AaY2rnSasfoIMVGJkVyFg2zUa6cCPaC3XlHcM6qX40Q5vKskGDR1e8yujLjB7VTby8nKjd9THjp6Za4BdcyuhTGwO9bNWkI+Qn8S6DicE7VNVsXOQC4OhJcqGe6iTHd2AOZgez5xxTuMcEdZxL6szLVCL/dRtW6rrdPkG6OvdNp29KCyfuDu0bClEuiRLJTywVF3b89BhAo8bpdDrcjOXVxF9Owr6INCiw1fLRN+4BKGuWR08aaJh08l9pVM+LMti2BRGx7Lenj22YL+3crSo3Sqz14WkO/PJtj5zVCSb9r2XP/BT09J7J1z72Ho5hrroYCc9mJiCt2kU+XLZ9jKjtIFaw7LW1kTxDok2LpkdbVKsWONyYUNXISovv6xBTwfQLNV+j//kVNbAq35AoHdU5PBjgT4Qht6w9ONGbwh1j9FgHV6nSKeAMV1l3NqfayZzOT3VBaFb7sBV2IeYmVM9mNHqLo8cCQPpBFrSlFjpXCr3Ps8p7jFmyzjh2yTeHJ0yA70mKqoPcBaXwivNhNyLmyXPhxy94dHFNQqK+L/DqwqueBVIvJoHhZfYm7dCg5OaA9V2nFuKsuTHDOqqHx7GmdDzVshwsYhgTQPmkHCb+oUelwJ65vqEwySgyKurivbnydomnkPngjenFROv57L4Auods2a8vUOuHSfODCpbdAO9/f/+xIOnByPdT5pQnaoxeH1GykPKvtVEJKfXvEd0ONzwepUN5lUrRhqxmIEIEzGzKuIxa49pnlu+mS5ZJej98LFouhl/7PR8jSbY2T7yHxF93UQShfsfTDJropKTUCUe9A+DzVmQDbiOJNW/b5hiGs0cD3Tx8fDDQWciwdRy/rpACaICPfls5z8OdeP0XemZZwg+ybMwxlpv3/ozIgmS35pEWZYwBZs2CIp0WuhfZPqPpGn8TmeKnwU/LKmTeSqkrzYjsjJQ7BQJxBPZURoNZ+8lP9ca7VpvvVO6xhxQyS/bWMINUVWDt7VWi/bgjL4IuqzbBCqMJaf+YGEhGTPbIvdk0bVAqW86q63FJG87tDIsVraHrdPoCvp//2Hql5pmeLmOhFco4SDM0KAflXukO2JP/zpPSqHPr+cCoxrMoxPHPsVZ+Yfbw/0tDu6x7qYtfKVjeg22Ujf0OX2lA2sPtx5CgwFVVfpNq2mventRFlH7HcUuF/xsZqmQtPQ+zbjx3UwHUDwczYkK/gzrSFqMnxUp9bZLfeTFPTO6RNXAy6vnpDg69S08xhe+QP59RYEWutgKorOWV1Isy0COzNSMKo4i3eryZKmKMKbSz4i/Tk2QGc2T7bRIhG5oSuJxgA39k3TraFNO37KAQGA1GyAULu2ezXY+T2Y2u8UUjE08gyTJUj1gmGHgHY7Py4VTpJ3nt1tcg3+tCrXisveGmx+ttDfPfa+9lfKoEHgrbeokLFF2gMICZdKoWFt+UhxrcqDfEiBkEDc2k0EUIlDkzCkW3mzl4EJm3dT3dIwYjWRBRzoBsFSnExin6XqHpPLSmUC97SCaZp+E3qKnqTBOIRBocflr9p+7whyltQiZz/ruxxOPfKdrdhOD0cgg1u3P6M+mVRjbjoL5ZZXVEtN+Y1cilAAAAFvqH2cBxn/m7Ht0EqD9b1t2PzSY60/E/8wobUaQOLtI/SRtlSWOennDMVAD9/O2VSV0Mk1DtcppmElKxEM7x+jN8knP896J79ibTt9SmH1jtCRAw7W3ppIPTE/vvpsei4mhFFZ4Zu9UXTD1F1PIDL6ZWBd62T4SMwcpJEXJDaDnveaUd5aKCUCKf3Xn/XukkhyGhPAerrZvcevlika94K01VkAR/jt23+4nzdx8z8Pc09kHvz2Hivnlrn19KUDAoZ+Y+Ug5QfyA+9Xv5VnxQ5MaJGr1Oa2s64skXage4me643sic7s5MrYCN7mkoFpqqiouHeOsW3YVrtJgapNnpSbG32oY7uIcLPbsqIlX0dewGaa7TD+96uhP+cC6SBX03428ydpJ6rvXxlHPFFHzcthPRrqA5+TB38Djbu81mrmUpSax0EC3Ebb5u6MPrDd0PtTQBYGU1H2bL+JnyB1ET93BR5u/N5M3/mDWegRjfUE2ijCxVZWil8o1TA7snG4PbeHisX3+6YIy4DeQlBVondPQ4RvsM8WShr5o0W1vJoMfMMcyZ4rP8dLhE0yiLYPeYQLDbKHJEa+kkHjrN0BshCAqBLl8XlnmhSLoN4wbu3gdkX8XQWqSPOYwon6Tf0zz/5xV7GQCrnr9srmEqJ9Mr1c1aPJA3th4rJk6P8ywPdxr3m9Gnne8VJYdJzirB9k2/0+RhT0mmi/4ebIa5dbFxi02tz5SGiNlwhuFxN8VD3XP56r8TvaBL/zc2BP0BBTRRbU5Ezlqk2L9l27iHmA4rS+g6VhICLmJOMM9oLssiS3SDkg8N0ZSFpYTSpxfUGTJpL8tsIHRVMs2VuZ+jsNt9MPj0TKIywTifvQcE8A96lWGOvDANHlgGQW7KVlg/AwwfhaYz2Uy4zCca9T3/NHZlN5P+0/LI34dukZTStzZZJfUvZa9aavMCXZpWLTajB1kxfkGPUn3y/kc5jGTogMZzzqJ7uXoGJK8g0TOWeADa7+h9WGaE30a37reLVhG9weuTiOZptugpdbhn0I+PrXzDLosNWRugasNRp3MImETN2jdrHZxdF/p+aK+V231aop5XbeuOMFkn3m1pCYxIbytEOmzkAyVOB+Kg5Q7VMF064I4TmyiNm4TzNVL/L1/HhjJexJvwT8UpbOKB7r7Sbm6xOHhsF5ccVfLWRqvjKOX8Ee6cmuy5VLYf7+s2OUupSe8e9BXd2R4KoUU5NrvkcwUTLZutXgp/ROjlnpadDufTlhnxcZBR+3UAjHM0pPE+Fo2lKWeQAVhpbFSN74Xhj0/xB3NxoPN2uGEPq1RcZyKneJtHwc1ObMCCZIRbeNynFoB55GR7NIoLE2c7zSS/4yQSKTWaFflQGfsDQ7DsppLCFSgFfEqU17Mt2NOpVsBzhDM+hClxlzVILoRbgrhQxj9ZLFxutbWtTNJrwA8J2u/Ijbr03PUe8oqM0mqOhmlOo/cHjf3z9WQnaM3Mm3rh2s7xCXaFsbjwd/XTue+zWg93WXZiNcwiL1VugPpg33Wx7TzAksZOl38ql9KZ6aRvsCLFPNQHhLNp8DaxUEUzFjCN1HHDqOmOt7T2o8cXfO0Drb4+fgQD68SUquPYD7WVjcWKv0HP8U+cbwYXrunPBOD8OAfy4ju40xv43igoas0g8ehFhI8pZbmcRak7X3PiNn3zWfvxwZ9HGInuHjW3FS+Nu/WJNHXZsybGgBFLrgrUdW682tc2zgMFCFauKrqmD1ROvDStgJuJUX9r8NEWAsLijIizC8/vs14VZAedZ260LQV5E345gHjW+gpcGRTG2xzruigWcsxoYcl79KkmcKfzGbHda/ehZcnpZE9LKJCjV2u2ZGxHMsPSY/W9iUARwzO4lWph0WmiD4Q+y3klc/BKzH2sw6Sxz1mh2PfMtN/b6bchHh2BzFuDRB0YgO7PXy8CG5+o3h153WVUAnQnykzDvl5Dnseq84fYH1SMU5aaO+4SPyd6SiCz/Txp1Yd8FLYSLa5RsY73vtk1+20z3O5moMgbe8L8ZThRGNh5MkGwTvSW+s/uQSBKgz6Z9O3qfFjfJLeXCBWhatRp1JtcYVxkdyKkpkVqsmG3DSBoodc9ijV0i5JUQpCuJ8z9twMS++HS+EX+VU/pyQDq4MjSn6Uw/Bd+JKAAvBsB0iTrL3GRhIFdkkI9SnUeUYw6jKLCMGQuvn4MKr5WS0jQzM6nhL0YZe8kF61XvRd3bjqdbSGyjd9zpI8KpxO95TR6tLiFa+sVtUpy8P+QiYd/sHJMVyH5QLBIyliFKq2kCwq1ilZX4lJ7TVxcQG5neBL+luwqmWCmuhY4NZao6T4+cy8+PlgzpoEeU57gbqvSdA4Fvo5+HFvNpS1/2Po/ntqaBSNfqaaByHG1/0FBhxKbZIoxAWeoQ+vXZkNM05d3VEjGAnf+o76LSetTX0b3g0NaCxYIeEnkaUyMpx7XRFgdSOqEadGu9VTOEowC/yqWcKUx1AxCD8GF5Ovw3TdwVaK4lXN/jJ58hiKXCbbTEg5JLRV2dSBnbwoMfiZP/opWZ+MjNvsk4mpqt2h02juZTclQnNsYeEyqKtLPArNqu6siCIXy72jJJaqjanpeQwuJLCD/eVHJ6mXKcKNZY+rZtyL+s3fBzQZAVS1FApYPs9rv+1R5PJD/ZF+ceTHdo+vWcx1x+/fPmeSRgo+0L7vFNnW8R+usZC4uPeU1uol3KgZy4XWHF69TajP/OCg7kzC1XzioSxq4UoE44a6t/AOsqwB/hjHylBx0Tdsbi6q4RGMLcmMD/vPpIhQo1uaxbnlSi9yfv1zsVj4fplSkBWDgofJ0cuN1TxpxG2Cv5TQTLWDb1Y3lGUBf8Qw0cd5uK4/ISRrVkX57CaIAAOM8Oe+n08uNlbN3JGmWzVHLCuAeMx6Af9S+pWcokF/cZJHjI04nmNyRUENLCCs0NpQbsJInQEp1o8oY+qtS2jod07XCc6f39LezaIU9H6wqDUXGx18lSxLsYNLDuydEuWPPGLFbFxt0/OYwn1Steqop3Cf5w8YveKMhv/1AGKx9FOX9fz1fKUu3APOukiZYUDMF/kS/EH/E6XyYZ7kzxznDZJP0uO+9gITv9hY+3+EOm9Sas1UpTkG9gWU8pIU56s4JNfg00UuxqjyVbTbMe4hJeaqxtyrInjozToiZQtSXJjyJvBLG/j/W5fPerNiTdxn73Mz73qRCC/mQAbaiAqHykV1Ouh2wKvFSju+W+qx9viYXA4hkH8Put6961ewCorYxC6WSz6JWHwAb/ma83OfV+ccNHSopxDl7Pfv5G5GFiLoNpWY/yMR0TmFVHjtFPSSYk8jqogWzbBddtE+Kxnxfz+pAxUgdPC2AGAV/ehuOE42u1dI1in5HaO+84/kQKU3tGD9mMh2pHDxxALDmEjkexSWRb4+JyT2VxWD6yTKu8sc41bXRXjKrnsx/kV+8WcyyhZWLBtRF7sknso4rRUJJ3rdvvc7PKxLcJSbtgZF1li1H1/g76G4qfHuiTIApXT9bzs/PPRww3Z9qW0VZJkaeD94Yqal9OTudq86s+VkxuiU63lnV7wP60bu074a2AlWjppOFVKpIzycfiMMkXAuPbGgq8RHryJJbp5mgLed21cXGZTdGLJVjfOz8q8H/Xmrv3omOIZ+6x2v738T+JmKPUpv/3PVadPXxqQjDANZg8KLhiFhi1ltNjeexNcqbSpuPFnR08mYxRCksoURc1+ZiR59f2AQnfK1hBP7shvjhHl7mBQ06D49dutiujAI6DSxyFlkqJ+zJWteV7jLL6SLx5XZ7bNW7WBOTtQy0Tg+ZiU/EPxXnvl+5NDhds0igxliYj6lWVfWV3wZxhpwxyhJHtFSriP30MBFw/QPymvRa8Im2wE7v+V0vJGhvZzKm3muxqkzy3wpUDARMshG77f/gIMEXufMSE7YiVDCs56RzNsfxDcMbtYvEksj9Il8m8lbaCkpteuVmzDBvrlISipisENq61JvT5DHr/8o9NnL0TAXn1rLBDQRg8hW5XZziHDiBbQHJWOlBRTHgPmqGt2rVdPOc3ou0rOoBmmiaq/H57MIVTLTdFgsuqU6pNp6DZjYbcQUlCZX9oh1ohoYSep4g/GD1RQE4IpKg1S/s+wu+lQZnLB1N91z8PFWXWqwCe18OYX1vQRPqBc5wF9+wYX0r8uSdAyNZ/uTss8J/ejj+9rJ3kfR5rvOuMTtjEVWNRF7VkVwXxVGJLiaZBx6GEc51/sLsZM5T7xow1grPT6WZi4ycZxJbrY42J30SF5uGspcTtopDcbkvL63x4d4hu2kLTNM0TBRTAZf1SjNfQ3kepHf9rKkVE2joOf6+7ZBUOg/o6qRaY4qGQPtq/r9xyxDCrVtBQuu1nqa9oLG2J0Kcj4BV/agXatJZiPB/AsxLsknT2QQY/ujsdTr87rFe6a7r3uPdv4n61arF1YRRO61lp9yxrWTlJIJsmkUgcxeamk3uZvy5rMnlM5/o6GSGQyo6DzeddRYSQ1tYbaEtXC9xsGJtrvsK+tEtqSz8+2l/Sn3uMeF2Esw3OdmISg4ixbeHAtJ+hjTUwjxIF3FplTzesKs3erISEPteRdNBGK7Ya23JnbRlZf/Bf3iY0KI9CqtKSZJU8H8NcxDyYkGZt30k5aVi/mIkfW+0rmWyHBne9jYSnS2GiJDHJuOyWZuTBjmDENRztaLmXMSqVtum28cLQVx2K9NGuPnanQrQS0VxDI0sehPvMQbCdozDq3TERffqNUv9xisweJ+oX+bYIhWAb6rsf1sIUqRTMoCIuD56guSJ0BjTVAjw63Ugv40nisVDE9rLvkTMxQ1t7KgXbXU8YOOYJmMkFqBgHmyfJhdqjyMzJytLolXabVjEu1zl1A0syybR3u/QV6vCCrX77b+rJfaNbHCVNKOZSmjWdLGvfJsbAJx/D1aXL0q/plCJcferV1be44J3FgVLk0401wAy0EVftoJEUtOt/nV8FEZXlV0Ba5TA9Qj9Dtp7n3DPo4SUHVRiyMOYm51lnuKes6l7xyH42pk6xRwgD4H084MLidTLzq/0DHPcPt2dOkcoUI+aghXqjb+GBPCY/rZcbf8eC/aIzFBhoWIe7Xyu2dbJbjgiq0J0ynHiuKAWQ3d8sk7TRZlpcXfKyYe1bq2LPxzI2PH/SbVm3X7Vhv3ULp8QlLbnF+7c0QDO7jMJWG27mPMK8DAPz1nYkkRz7lPu1vtOL0dwcgbm3JV1QXifyAUdiEyQS7fWYaCflY0DJ/uyh9WyGnGk/y3VF8isOVqTZiEvTMSWPWp6ptBbbT9idcGFsHWHuDoBZH6q69gv2y8BODvpSCjKl0tZfblfwAPp4zZ9zAAAA=='}}}}


'''
Code only runs if the total number of table rows is not less than 'trigger_size'.
The number of table rows gets reduced to the 'max_rows' size.
This is done by deleting the first rows in the table.

The database filesize then gets reduced by calling the compact_api_labels function.
This runs the 'VACUUM' statement (compact).

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



def main() -> None:
    # orders = label_orders_list('202309261045')
    orders = label_orders_list()
    # pprint([item['orderId'] for item in orders])
    # sys.exit()

    sku_values = [item['sku'] for item in orders]
    products = products_lkup(sku_values)
    orders = orders_grouped_items(orders)
    # pprint(orders)
    orders = get_item_properties(orders, products)
    
    '''
    with open('api_labels_path.txt', 'w', encoding='utf-8') as file:
        file.write(DB_PATH + 'api_labels.db3' + '\n')
    '''

    pprint([item['orderId'] for item in orders])
    # sys.exit()

    id_label_dict = {}
    for order in orders:
        pprint(order)
        # with open('debug_order.txt', 'a', encoding='utf-8') as json_file1:
        #     json.dump(order, json_file1, indent=4)
        
        if False is order['getbox']:
            continue

        order = shipment_request_details(order)
        # pprint(order)
        # sys.exit()

        if order:
            if len(TEST_DATETIMES) > 0 and order['AmazonOrderId'] not in TEST_DATETIMES:
                continue

            order_id = order['AmazonOrderId']
            label_data = ''
            id_label_dict[order_id] = label_data

            inc = 0
            while True:
                try:
                    label = request_label(order)
                    label = dict(label.__dict__)
                    # pprint(label)

                    if label['errors'] is not None:
                        break

                    order_id = label['payload']['AmazonOrderId']

                    if order_id in id_label_dict:
                        label_data = label['payload']['Label']['FileContents']['Contents']
                        id_label_dict[order['AmazonOrderId']] = label_data
                        break
                
                except Exception as e:
                    pprint(f'An error occurred: {e}')
                    with open('debug_label_data.txt', 'a', encoding='utf-8') as json_file2:
                        json.dump(f'An error occurred: {e}', json_file2, indent=4)
                
                time.sleep(REQUESTS_PER_SECOND)

                # Max of 3 loops
                inc += 1
                if inc > 3:
                    break
            
            # if 'payload' in label and 'AmazonOrderId' in label['payload']:

            pprint({'order_id': order_id, 'label_data': label_data})
            # with open('debug_label_data.txt', 'a', encoding='utf-8') as json_file2:
            #     json.dump({'order_id': order_id, 'label_data': label_data}, json_file2, indent=4)

    
    pprint(id_label_dict)
    
    '''
    Connect to 'api_labels.db3' database.
    Required to insert the `orderID` and `label_data` values
    into the `prime_labels` table.
    '''
    api_labels_conn = sqlite3.connect(DB_PATH + 'api_labels.db3')
    api_labels_cur = api_labels_conn.cursor()

    for order_id, label_data in id_label_dict.items():
        if '' != label_data:
            api_labels_cur.execute("INSERT INTO `prime_labels` (`orderID`, `label_data`) VALUES (?,?)", (order_id, label_data))
        else:
            api_labels_cur.execute("INSERT INTO `prime_labels_missing` (`orderID`, `label_data`) VALUES (?,?)", (order_id, label_data))

    api_labels_conn.commit()
    api_labels_cur.close()
    api_labels_conn.close()

    # Resize table. If total table rows has reached 6000, reduce to 5000 (removes first rows)
    # Database also gets compacted (VACUUM) to reduced filesize.
    limit_tbl_rows(6000, 5000, DB_PATH + 'api_labels.db3', 'prime_labels')


if __name__ == "__main__":
    main()
