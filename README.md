# Amazon SP API (Selling Partner API)

## General notes

The Amazon APIs that pull orders and create Prime labels are coded in Python and use the `python-amazon-sp-api` package. They are located on the Ubuntu 20.04.3 LTS server (192.168.0.125).

`/var/www/html/sp_api/amazon_sp_api_get_orders.py` runs at 40 mins past the hour, and saves the orders to `FespMVC/NEW_API_SYSTEM/sp_api/api_orders.db3`. This is not the `api_orders.db3` database that the other platform APIs write to `FespMVC/NEW_API_SYSTEM/api_orders.db3`. Originally, it was set-up to use the same database, but writing to the database from different servers resulted in the database records becoming corrupt.

The data from `sp_api/api_orders.db3` gets copied to `api_orders.db3` by `NEW_API_SYSTEM/sp_api/pull_amazon_orders.php` on the hour. 2 fields are not copied: `datetime (amazon_orders)`, `orderItemId (amazon_items)`. These fields are required by the `get_amazon_label_NEW.py` script.

At 55 mins past the hour `/var/www/html/sp_api/get_amazon_label_NEW.py` runs. This script calculates which orders need Prime labels then creates order shipments and retrieves their labels.

The crontab schedules can be viewed by SSHing into 'vova@192.168.0.125' and running `crontab -l`. Alternatively, `crontab -e` to edit schedules.

### get_amazon_label_NEW.py

