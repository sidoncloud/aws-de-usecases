mysql --local-infile=1 -h ecommerce-db.c1yklxlrd3gk.us-east-1.rds.amazonaws.com -P 3306 -u admin -p

CREATE TABLE orders (
    order_id INT NOT NULL PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50),
    created_at DATETIME(6),
    shipped_at DATETIME(6),
    delivered_at DATETIME(6),
    num_of_item INT,
    last_updated_ts DATETIME(6)
);


LOAD DATA LOCAL INFILE "orders-1804.csv" INTO TABLE ecom_db.orders FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

UPDATE orders
SET 
    last_updated_ts = NOW(),
    delivered_at = NOW(),
    status = 'Complete' where status ='Shipped';