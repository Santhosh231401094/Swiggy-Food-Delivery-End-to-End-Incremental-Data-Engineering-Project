-- 16. Top Rated: Which restaurants have an average rating higher than 4.5?
SELECT restaurant_name, AVG(f.rating_numeric) AS avg_rating
FROM facts f 
LEFT JOIN dim_restaurant r 
ON f.restaurant_key=r.restaurant_key
GROUP BY r.restaurant_name
HAVING AVG(f.rating_numeric)>4.5
ORDER BY avg_rating;

-- 17. Cuisine Satisfaction: Which cuisine has the highest average customer rating?
SELECT r.cuisine_type,AVG(f.rating_numeric) AS avg_rating
FROM facts f 
LEFT JOIN dim_restaurant r
ON f.restaurant_key=r.restaurant_key
GROUP BY r.cuisine_type
ORDER BY avg_rating DESC;

-- 18. Rating vs. Spend: Is there a correlation between high spending and high ratings?
SELECT rating_numeric, AVG(total_amount) as avg_spendings
FROM facts 
WHERE rating_numeric IS NOT NULL
GROUP BY rating_numeric
ORDER BY rating_numeric;

-- 19. Low Rating Audit: List the restaurants with more than 10 orders that have an average rating below 3.0
SELECT r.restaurant_name,COUNT(f.order_id) AS orders,AVG(f.rating_numeric) AS avg_rating
FROM facts f 
LEFT JOIN dim_restaurant r
ON f.restaurant_key=r.restaurant_key
GROUP BY r.restaurant_name
HAVING COUNT(f.order_id)>10 AND AVG(f.rating_numeric)<3.0
ORDER BY avg_rating DESC;

-- 20. City-Wide Quality: What is the average rating of food delivery across the entire city of Bengaluru?
SELECT AVG(f.rating_numeric) AS avg_rating
FROM facts f
LEFT JOIN dim_location l
ON f.location_key=l.location_key
WHERE l.city='Bengaluru';


