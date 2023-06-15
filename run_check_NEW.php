<?php

/*
http://192.168.0.24/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/run_check_NEW.php

*/

$db_api_orders = new PDO('sqlite:../api_orders.db3');
$db_api_orders_NEW = new PDO('sqlite:api_orders_NEW.db3');



$rowid = orderID_to_rowID($db_api_orders, 'api_orders', '202-1477242-7357943');
$api_orders_orderIDs = select($db_api_orders, 'api_orders', $rowid);
// echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($api_orders_orderIDs); echo '</pre>'; die();

$rowid = orderID_to_rowID($db_api_orders_NEW, 'api_orders', '202-1477242-7357943');
$api_orders_NEW_orderIDs = select($db_api_orders_NEW, 'api_orders', $rowid);
// echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($api_orders_NEW_orderIDs); echo '</pre>';



$args = [
    'dbObj' => $db_api_orders_NEW,
    'requiredOrderIDs' => $api_orders_NEW_orderIDs,
    'displayOrderIDs' => true,
];

$data_NEW = groupOrderIDsByTimestamp($args);

$data_NEW_all_orderIDs = convertOrderIDsToKeys($data_NEW);



// echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($data_NEW_all_orderIDs); echo '</pre>'; die();
// echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($data_NEW); echo '</pre>'; die();


$args = [
    'dbObj' => $db_api_orders,
    'requiredOrderIDs' => $api_orders_orderIDs,
    'displayOrderIDs' => true,
];
$data_LIVE = groupOrderIDsByTimestamp($args);



$data_LIVE = missingOrderIDs($data_LIVE, $data_NEW_all_orderIDs);
echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($data_LIVE); echo '</pre>';








//=========================================================================
// FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
//=========================================================================

function missingOrderIDs($array, $orderIdKeys)
{
    $tmp = [];
    $tmp2 = [];
    foreach ($array as $ts => $arr) {
        $count = 0;
        foreach ($arr as $i => $orderID) {
            if (isset($orderIdKeys[$orderID])) {
                $tmp[$ts][$i] = $orderID;
            }
            else {
                $tmp[$ts][$i] = $orderID . ' -> MISSING';
                $count++;
            }
            
            // $tmp[$ts][$i] = isset($orderIdKeys[$orderID]) ? $orderID : $orderID . ' -> MISSING';
        }
        $tmp2[$ts . " / $count MISSING"] = $tmp[$ts];
    }
    return $tmp2;
}

function convertOrderIDsToKeys($array)
{
    $tmp = [];
    foreach ($array as $arr) {
        $tmp = array_merge($tmp, $arr);
    }
    return array_flip($tmp);
}

function groupOrderIDsByTimestamp($args)
{
    $dbObj = $args['dbObj'];
    $displayOrderIDs = isset($args['displayOrderIDs']) ? $args['displayOrderIDs'] : NULL;
    
    $whereIn = '';
    if (isset($args['requiredOrderIDs'])) {
        $requiredOrderIDsStr = implode("','", $args['requiredOrderIDs']);
        $whereIn = " AND `orderId` IN ('$requiredOrderIDsStr')";
    }
    
    $sql = "SELECT orderId,timestamp FROM `save_queue_records` WHERE `platform` = 'amazon'$whereIn";
    $results = $dbObj->query($sql);
    $results_save_queue_records = $results->fetchAll(PDO::FETCH_ASSOC);
    
    $datetime_keys_orderIDs_arr = [];
    foreach ($results_save_queue_records as $rec) {
        $datetime_keys_orderIDs_arr[date('Y-m-d H:i', $rec['timestamp'])][] = $rec['orderId'];
    }
    
    $datetime_total_orderIDs = [];
    foreach ($datetime_keys_orderIDs_arr as $key => $arr) {
        $datetime_total_orderIDs[$key] = count($arr);
    }
    
    if ($displayOrderIDs) {
        return $datetime_keys_orderIDs_arr;
    }
    else {
        return $datetime_total_orderIDs;
    }
}

function orderID_to_rowID($dbObj, $db, $orderID)
{
    $sql ="SELECT rowid FROM `amazon_orders` WHERE `orderId` = '$orderID'";
    $results = $dbObj->query($sql);
    return $results->fetchAll(PDO::FETCH_COLUMN)[0];
}

function select($dbObj, $db, $rowid)
{
    $sql ="SELECT orderId FROM `amazon_orders` WHERE `rowid` >= $rowid";
    $results = $dbObj->query($sql);
    return $results->fetchAll(PDO::FETCH_COLUMN);
}