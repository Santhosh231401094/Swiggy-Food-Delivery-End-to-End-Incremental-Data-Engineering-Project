-- 6. Average Delivery Time: What is the average delivery time (in minutes) for all online orders?

SELECT AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time))
FROM facts f
LEFT JOIN dim_date d
ON f.date_key=d.date_key
WHERE f.online_order='Yes' AND f.delivery_time IS NOT NULL;

-- 7. Fastest Restaurants: Which 5 restaurants have the fastest average delivery time?
SELECT TOP 5
    r.restaurant_name,
    AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) AS avg_delivery_time
FROM facts f
LEFT JOIN dim_restaurant r
ON f.restaurant_key=r.restaurant_key
LEFT JOIN dim_date d
ON f.date_key=d.date_key
WHERE f.online_order='Yes' AND f.delivery_time IS NOT NULL
GROUP BY r.restaurant_name
ORDER BY avg_delivery_time ASC;

-- 8. Location Delays: Which location has the highest average delivery time?
SELECT TOP 1
    r.restaurant_name,
    AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) AS avg_delivery_time
FROM facts f
LEFT JOIN dim_restaurant r
ON f.restaurant_key=r.restaurant_key
LEFT JOIN dim_date d
ON f.date_key=d.date_key
WHERE f.online_order='Yes' AND f.delivery_time IS NOT NULL
GROUP BY r.restaurant_name
ORDER BY avg_delivery_time DESC;

-- 9. Distance Impact: What is the average delivery time for orders traveling more than 10km?

SELECT 
    AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) AS avg_delivery_time
FROM facts f
LEFT JOIN dim_date d
ON f.date_key=d.date_key
WHERE f.online_order='Yes' AND f.delivery_time IS NOT NULL AND distance_km >10;

-- 10. Delivery Success Rate: How many orders have a valid delivery_time compared to the total online_order = 'Yes' count?
SELECT
COUNT(delivery_time)*1.0/COUNT(*)*100 AS Deliverysucessrate
FROM facts 
WHERE online_order='Yes';



