-- PRODUCT CATEGORIES

-- view_table('products', 5)
SELECT *
FROM products
LIMIT 5;


-- ranked_categories
SELECT
    product_category_name_english AS category,
    SUM(price) AS sales,
    RANK() OVER (ORDER BY SUM(price) DESC) AS rank
FROM order_items
    JOIN orders USING (order_id)
    JOIN products USING (product_id)
    JOIN product_category_name_translation USING (product_category_name)
WHERE order_status = 'delivered'
GROUP BY product_category_name_english;


-- category_sales_summary
WITH RankedCategories AS (
    SELECT
        product_category_name_english AS category,
        SUM(price) AS sales,
        RANK() OVER (ORDER BY SUM(price) DESC) AS rank
    FROM order_items
        JOIN orders USING (order_id)
        JOIN products USING (product_id)
	JOIN product_category_name_translation USING (product_category_name)
    WHERE order_status = 'delivered'
    GROUP BY product_category_name_english
)
-- Top 18 categories by sales
SELECT
    category,
    sales
FROM RankedCategories
WHERE rank <= 18
-- Other categories, aggregated
UNION ALL
SELECT
    'Other categories' AS category,
    SUM(sales) AS sales
FROM RankedCategories
WHERE rank > 18;


-- categories_by_median
WITH OrderedCategories AS (
    SELECT
        product_weight_g AS weight,
        product_category_name_english AS category,
        ROW_NUMBER() OVER(PARTITION BY product_category_name_english ORDER BY product_weight_g)
            AS category_row_n,
        COUNT(*) OVER(PARTITION BY product_category_name_english) AS category_count
    FROM
        products
        JOIN order_items USING (product_id)
        JOIN product_category_name_translation USING (product_category_name)
    WHERE product_category_name_english IN ('health_beauty', 'watches_gifts', 'bed_bath_table',
        'sports_leisure', 'computers_accessories', 'furniture_decor', 'housewares', 'cool_stuff',
        'auto', 'toys', 'garden_tools', 'baby', 'perfumery', 'telephony', 'office_furniture',
        'stationery', 'computers', 'pet_shop')
)
SELECT category
FROM OrderedCategories
WHERE
    -- Odd number of products: Select the middle row
    (category_count % 2 = 1 AND category_row_n = (category_count + 1) / 2) OR
    -- Even number of products: Select the two middle rows to be averaged
    (category_count % 2 = 0 AND category_row_n IN ((category_count / 2), (category_count / 2 + 1)))
GROUP BY category
ORDER BY AVG(weight);
