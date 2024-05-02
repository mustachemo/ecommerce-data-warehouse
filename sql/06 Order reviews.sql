-- ORDER REVIEWS 

-- view_table('order_reviews', 5)
SELECT *
FROM order_reviews
LIMIT 5;


-- review_score_count
SELECT
    review_score,
    COUNT(*) AS count
FROM order_reviews
GROUP BY review_score;


-- negative_comments
SELECT GROUP_CONCAT(review_comment_message, ' ') AS comments
FROM order_reviews
WHERE review_score IN (1,2);