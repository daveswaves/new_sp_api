<?php

/*
http://192.168.0.24/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/title_difference.php
*/



$db_api_orders_NEW = new PDO('sqlite:api_orders_NEW.db3');
$db_api_orders = new PDO('sqlite:../api_orders.db3');

/*
$sql = "SELECT * FROM `amazon_items` WHERE `orderId` = '203-4271815-8815501'";
$results = $db_api_orders->query($sql);
$results_api_orders =  $results->fetchAll(PDO::FETCH_ASSOC);

$sql = "SELECT * FROM `amazon_items` WHERE `orderId` = '203-4271815-8815501'";
$results = $db_api_orders_NEW->query($sql);
$results_api_orders_NEW =  $results->fetchAll(PDO::FETCH_ASSOC);

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($results_api_orders); echo '</pre>';
echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($results_api_orders_NEW); echo '</pre>';
*/

$results_api_orders_NEW = select($db_api_orders_NEW);
// $orderIDs = array_column($results_api_orders_NEW, 'orderId');
$orderIDs = array_keys($results_api_orders_NEW);


$orderIDs_in = implode("','", $orderIDs);

$results_api_orders = select($db_api_orders, $orderIDs_in);

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">Total orders in new SP API database: '; echo count($results_api_orders_NEW); echo ' | '; echo count($results_api_orders); echo ' of these exist in live database.</pre>';

$missing_orderIds = [];
$orders_with_different_titles = [];
$identical_orders = [];
foreach ($results_api_orders_NEW as $orderId => $title) {
    if (!isset($results_api_orders[$orderId])) {
        $missing_orderIds[] = $orderId;
    }
    else if ($title != $results_api_orders[$orderId]) {
        $orders_with_different_titles[$orderId] = [
            'api_orders    ' => $title,
            'api_orders_new' => $results_api_orders[$orderId],
        ];
    }
    else if ($title == $results_api_orders[$orderId]) {
        $identical_orders[$orderId] = [
            'api_orders    ' => $title,
            'api_orders_new' => $results_api_orders[$orderId],
        ];
    }
}

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">Orders in Python SP API database missing from live system: '; print_r($missing_orderIds); echo '</pre>';

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;"># Total orders with different titles: '; echo count($orders_with_different_titles); echo ' | # Total identical orders: '; echo count($identical_orders); echo '</pre>';

echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">Orders with differnt titles: '; print_r($orders_with_different_titles); echo '</pre>';
echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">Identical orders: '; print_r($identical_orders); echo '</pre>';

//=========================================================================
// FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
//=========================================================================
function select($dbObj, $where_in = NULL)
{
    $sql = !$where_in ? "SELECT orderId,title FROM `amazon_items`" : "SELECT orderId,title FROM `amazon_items` WHERE `orderId` IN ('$where_in')";
    
    // echo '<pre style="background:#111; color:#b5ce28; font-size:11px;">'; print_r($sql); echo '</pre>';
    
    $results = $dbObj->query($sql);
    return $results->fetchAll(PDO::FETCH_KEY_PAIR);
}
