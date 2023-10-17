<?php
/*
http://192.168.0.24/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/pull_amazon_orders.php
*/

// Runs at 55 minutes past the hour

ini_set('display_errors', '1');
ini_set('display_startup_errors', '1');
error_reporting(E_ALL);

$workingPath = dirname(realpath( __FILE__ ));
chdir($workingPath);
set_include_path($workingPath);

$sp_api_orders_path = 'C:/inetpub/wwwroot/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/sp_api/api_orders.db3';
$api_orders_path = 'C:/inetpub/wwwroot/FESP-REFACTOR/FespMVC/NEW_API_SYSTEM/api_orders.db3';

$sp_api_orders = new PDO('sqlite:'.$sp_api_orders_path);
$api_orders = new PDO('sqlite:'.$api_orders_path);

$sql = "SELECT `orderId` FROM `amazon_orders`";
// $sql = "SELECT `orderId` FROM `amazon_orders` ORDER BY `rowid` LIMIT 100000";
$results = $api_orders->query($sql);
$existing_orderIds = $results->fetchAll(PDO::FETCH_COLUMN);

$sql = "SELECT `orderId` FROM `amazon_orders`";
$results = $sp_api_orders->query($sql);
$sp_api_orderIds = $results->fetchAll(PDO::FETCH_COLUMN);

$new_oderIds = array_diff($sp_api_orderIds, $existing_orderIds);

$in = implode("','", $new_oderIds);

// 'datetime' field is not copied accross
$sql = "SELECT `orderId`,`total`,`currency`,`date`,`buyer`,`phone`,`email`,`isprime`,`service`,`shippingName`,`addressLine1`,`addressLine2`,`city`,`county`,`countryCode`,`postcode` FROM `amazon_orders` WHERE `orderId` IN ('$in')";
$results = $sp_api_orders->query($sql);
$sp_api_amazon_orders = $results->fetchAll(PDO::FETCH_ASSOC);

// 'orderItemId' field is not copied accross
$sql = "SELECT `orderId`,`itemId`,`sku`,`qty`,`title`,`variations`,`price`,`shipping` FROM `amazon_items` WHERE `orderId` IN ('$in')";
$results = $sp_api_orders->query($sql);
$sp_api_amazon_items = $results->fetchAll(PDO::FETCH_ASSOC);

$sql = "SELECT * FROM `last_request_timestamp`";
$results = $sp_api_orders->query($sql);
$sp_api_last_request_timestamp = $results->fetchAll(PDO::FETCH_ASSOC);

$sql = "SELECT * FROM `stats`";
$results = $sp_api_orders->query($sql);
$sp_api_stats = $results->fetchAll(PDO::FETCH_ASSOC);

$sql = "SELECT * FROM `stats`";
$results = $api_orders->query($sql);
$stats = $results->fetchAll(PDO::FETCH_ASSOC);

$stats_lkp = [];
foreach ($stats as $rec) {
    $str = $rec['platform'].$rec['time_start'].$rec['time_finish'].$rec['total'].$rec['incomplete_orders'].$rec['csv'];
    $stats_lkp[$str] = 1;
}

$sql = "SELECT `sku` FROM `missing_sku_info`";
$results = $api_orders->query($sql);
$missing_skus = $results->fetchAll(PDO::FETCH_COLUMN);

$sql = "SELECT * FROM `missing_sku_info`";
$results = $sp_api_orders->query($sql);
$sp_api_missing_sku_info = $results->fetchAll(PDO::FETCH_ASSOC);

$sp_api_missing_skus = array_column($sp_api_missing_sku_info, 'sku');
$new_missing_skus = array_diff($sp_api_missing_skus, $missing_skus);


$stmt = $api_orders->prepare("INSERT INTO `amazon_orders` (
    `orderId`,
    `total`,
    `currency`,
    `date`,
    `buyer`,
    `phone`,
    `email`,
    `isprime`,
    `service`,
    `shippingName`,
    `addressLine1`,
    `addressLine2`,
    `city`,
    `county`,
    `countryCode`,
    `postcode`
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

$api_orders->beginTransaction();
foreach ($sp_api_amazon_orders as $rec) {
    $stmt->execute( array_values($rec) );
}
$api_orders->commit();



$stmt = $api_orders->prepare("INSERT INTO `amazon_items` (
    `orderId`,
    `itemId`,
    `sku`,
    `qty`,
    `title`,
    `variations`,
    `price`,
    `shipping`
) VALUES (?,?,?,?,?,?,?,?)");

$api_orders->beginTransaction();
foreach ($sp_api_amazon_items as $rec) {
    $stmt->execute( array_values($rec) );
}
$api_orders->commit();



$time_start = $sp_api_last_request_timestamp[0]['time_start'];
$time_finish = $sp_api_last_request_timestamp[0]['time_finish'];

$stmt = $api_orders->prepare("UPDATE `last_request_timestamp` SET `time_start` = ?, `time_finish` = ? WHERE `platform` = ?");
$api_orders->beginTransaction();
$stmt->execute([$time_start, $time_finish, 'amazon']);
$api_orders->commit();


// Add to `queue_orderIds`
$stmt = $api_orders->prepare("INSERT INTO `queue_orderIds` (`platform`,`orderId`) VALUES (?,?)");
$api_orders->beginTransaction();
foreach ($new_oderIds as $orderId) {
    $stmt->execute(['amazon', $orderId]);
}
$api_orders->commit();




//=========================================================================
// This data should not be pulled (table needs removing in SP API).
// Values need to be calculated in this script by working out what is
// already in main database.
//=========================================================================
// Add sp_api_stats
$stmt = $api_orders->prepare("INSERT INTO `stats` (`platform`,`time_start`,`time_finish`,`total`,`incomplete_orders`,`csv`) VALUES (?,?,?,?,?,?)");
$api_orders->beginTransaction();
foreach ($sp_api_stats as $rec) {
    $str = $rec['platform'].$rec['time_start'].$rec['time_finish'].$rec['total'].$rec['incomplete_orders'].$rec['csv'];
    
    if (!isset($stats_lkp[$str])) {
        $stmt->execute(array_values($rec));
    }
}
$api_orders->commit();

$stmt = $api_orders->prepare("INSERT INTO `missing_sku_info` (`sku`,`timestamp`) VALUES (?,?)");
$api_orders->beginTransaction();
foreach ($sp_api_missing_sku_info as $rec) {
    $sku = $rec['sku'];
    $ts = $rec['timestamp'];
    
    if (!isset($new_missing_skus[$sku])) {
        $stmt->execute([$sku, $ts]);
    }
}
$api_orders->commit();
