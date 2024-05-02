-- SELLERS

-- view_table('sellers', 5)
SELECT *
FROM sellers
LIMIT 5;


-- seller_review_scores_and_sales
SELECT 
    sellers.seller_id,
    AVG(order_reviews.review_score) AS avg_review_score,
    SUM(order_items.price) AS total_sales,
    COUNT(orders.order_id) AS num_orders
FROM 
    sellers
    LEFT JOIN order_items ON sellers.seller_id = order_items.seller_id
    LEFT JOIN orders ON order_items.order_id = orders.order_id
    LEFT JOIN order_reviews ON orders.order_id = order_reviews.order_id
GROUP BY 
    sellers.seller_id
HAVING 
    COUNT(orders.order_id) > 10;


-- sellers_per_bucket
WITH BucketedSellers AS (
    SELECT
        seller_id,
        CASE 
	    WHEN COUNT(order_id) BETWEEN 1 AND 9 THEN '1-9 orders'
	    WHEN COUNT(order_id) BETWEEN 10 AND 99 THEN '10-99 orders'
	    WHEN COUNT(order_id) BETWEEN 100 AND 999 THEN '100-999 orders'
	    ELSE '1000+ orders'
        END AS bucket
    FROM order_items
    GROUP BY seller_id
)
SELECT 
    bucket,
    COUNT(seller_id) AS seller_count
FROM BucketedSellers
GROUP BY bucket;


-- seller_shipping_times (uses the same CTE as before)
WITH BucketedSellers AS (
    SELECT
        seller_id,
        CASE 
	    WHEN COUNT(order_id) BETWEEN 1 AND 9 THEN '1-9 orders'
	    WHEN COUNT(order_id) BETWEEN 10 AND 99 THEN '10-99 orders'
	    WHEN COUNT(order_id) BETWEEN 100 AND 999 THEN '100-999 orders'
	    ELSE '1000+ orders'
        END AS bucket
    FROM order_items
    GROUP BY seller_id
)
SELECT
    bucket,
    BucketedSellers.seller_id,
    JULIANDAY(order_delivered_customer_date) - JULIANDAY(order_purchase_timestamp)
        AS delivery_time
FROM orders
    JOIN order_items USING (order_id)
    JOIN BucketedSellers USING (seller_id)
WHERE order_status = 'delivered';

