<?php

/*
http://localhost/ELIXIR/check_api_orders_bds/run_check.php

http://192.168.0.24/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/run_check.php

http://192.168.0.125/sp_api/run_check.php
 */

$db_api_orders = new PDO('sqlite:../api_orders.db3');
$db_api_orders_NEW = new PDO('sqlite:api_orders_NEW.db3');

/*
// Display dates/times orders get pulled and how many
$sql = "SELECT orderId,timestamp FROM `save_queue_records` WHERE `platform` = 'amazon'";
$results = $db_api_orders_NEW->query($sql);
$results_save_queue_records = $results->fetchAll(PDO::FETCH_ASSOC);

$datetime_keys_orderIDs_arr = [];
foreach ($results_save_queue_records as $rec) {
    $datetime_keys_orderIDs_arr[date('Y-m-d H:i', $rec['timestamp'])][] = $rec['orderId'];
}

$tmp = [];
foreach ($datetime_keys_orderIDs_arr as $key => $arr) {
    $tmp[$key] = count($arr);
}

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($tmp); echo '</pre>';
echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($datetime_keys_orderIDs_arr); echo '</pre>'; die();
*/


/*
$sql = "SELECT rowid, orderId FROM `amazon_orders` ORDER BY `rowid` DESC LIMIT 1000";
$results = $db_api_orders_NEW->query($sql);
$results =  $results->fetchAll(PDO::FETCH_ASSOC);

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($results); echo '</pre>'; die();
*/
// 202-1477242-7357943
// rowid > 757237




$results_api_orders = select($db_api_orders, 'api_orders');
$results_api_orders_NEW = select($db_api_orders_NEW, 'api_orders_NEW');

$errors = false;





$array_diff = array_diff($results_api_orders, $results_api_orders_NEW);
if (count($array_diff)) {
    $errors = true;
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r("The following order(s) exist in 'api_orders.db3', but not in 'api_orders_NEW.db3':"); echo '</pre>';
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($array_diff); echo '</pre>';
}

$array_diff = array_diff($results_api_orders_NEW, $results_api_orders);
if (count($array_diff)) {
    $errors = true;
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r("The following order(s) exist in 'api_orders_NEW.db3', but not in 'api_orders.db3':"); echo '</pre>';
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($array_diff); echo '</pre>';
}



$diff_records = hashAllFields($db_api_orders, $db_api_orders_NEW, 'amazon_orders');


if (count($diff_records)) {
    $errors = true;
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r('The following orders (\'amazon_orders\') have different values:'); echo '</pre>';
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($diff_records); echo '</pre>';
}


$diff_records = hashAllFields($db_api_orders, $db_api_orders_NEW, 'amazon_items');

if (count($diff_records)) {
    $errors = true;
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r('The following orders (\'amazon_items\') have different values:'); echo '</pre>';
    echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($diff_records); echo '</pre>';
}

if (!$errors) {
    echo '<pre style="color:#111; font-size:50px;">'; print_r("Excellent!\nBoth DBs are identical."); echo '</pre>';
}



//=========================================================================
// FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
//=========================================================================
function select($dbObj, $db)
{
    if ('api_orders' == $db) {
        $sql ="SELECT orderId FROM `amazon_orders` WHERE `rowid` > 757237";
    }
    elseif ('api_orders_NEW' == $db) {
        $sql ="SELECT orderId FROM `amazon_orders` WHERE `rowid` > 4842";
    }
        
    $results = $dbObj->query($sql);
    return $results->fetchAll(PDO::FETCH_COLUMN);
}

function hashAllFields($dbObj1, $dbObj2, $tbl)
{
    $limit = '';
    // $limit = ' LIMIT 3';
    
    $sql = "SELECT * FROM `$tbl`$limit";
    $results = $dbObj1->query($sql);
    $results = $results->fetchAll(PDO::FETCH_ASSOC);
    
    $api_orders_hash = [];
    foreach ($results as $rec) {
        if ('amazon_orders' == $tbl) {
            $allFields = $rec['total'].$rec['currency'].$rec['date'].$rec['buyer'].$rec['phone'].$rec['email'].$rec['isprime'].$rec['service'].$rec['shippingName'].$rec['addressLine1'].$rec['addressLine2'].$rec['city'].$rec['county'].$rec['countryCode'].$rec['postcode'];
        }
        elseif ('amazon_items' == $tbl) {
            $allFields = $rec['itemId'].$rec['sku'].$rec['qty'].$rec['title'].$rec['variations'].$rec['price'].$rec['shipping'];
        }
        
        $hash = hash('md5', $allFields);
        $api_orders_hash[$rec['orderId']] = $hash;
    }
    
    $sql = "SELECT * FROM `$tbl`$limit";
    $results = $dbObj2->query($sql);
    $results = $results->fetchAll(PDO::FETCH_ASSOC);
    
    $api_orders_NEW_hash = [];
    foreach ($results as $rec) {
        if ('amazon_orders' == $tbl) {
            $allFields = $rec['total'].$rec['currency'].$rec['date'].$rec['buyer'].$rec['phone'].$rec['email'].$rec['isprime'].$rec['service'].$rec['shippingName'].$rec['addressLine1'].$rec['addressLine2'].$rec['city'].$rec['county'].$rec['countryCode'].$rec['postcode'];
        }
        elseif ('amazon_items' == $tbl) {
            $allFields = $rec['itemId'].$rec['sku'].$rec['qty'].$rec['title'].$rec['variations'].$rec['price'].$rec['shipping'];
        }

        $hash = hash('md5', $allFields);
        $api_orders_NEW_hash[$rec['orderId']] = $hash;
    }
    
    // echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($api_orders_hash['204-9110143-5806714']); echo '</pre>';
    
    $diff_records = [];
    foreach ($api_orders_hash as $orderID => $hash) {
        if (isset($api_orders_NEW_hash[$orderID])) {
            if ($hash != $api_orders_NEW_hash[$orderID]) {
                $diff_records[] = $orderID;
            }
        }
    }
    
    return $diff_records;
}
