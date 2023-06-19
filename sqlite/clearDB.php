<?php
/*
http://localhost/sp_api_local/sqlite/clearDB.php
*/

$db = new PDO('sqlite:api_orders_NEW.db3');

clearDb([
    'amazon_orders',
    'queue_orderIds',
    'save_queue_records',
    'stats',
    'amazon_items',
    'missing_orderIds',
    'missing_sku_asin_vars',
], $db);

$time = date('H:i:s');

echo '<pre style="font-size:44px;">'; print_r("'api_orders_NEW.db3' cleared! <span style='font-size:16px;'>[$time]</span>"); echo '</pre>';


function clearDb($tbls, $db)
{
    foreach ($tbls as $tbl) {
        $sql = "DELETE FROM `$tbl`";
        $db->query($sql);
    }
    
    $db->query('VACUUM');
}
