--DECISION SYSTEM

--Rule-based recommendation for total revenue
SELECT 
    l.area_name,
    SUM(f.total_amount) AS revenue,

    CASE 
        WHEN SUM(f.total_amount) < 50000 
            THEN 'Increase marketing & offers'
        WHEN SUM(f.total_amount) BETWEEN 50000 AND 150000 
            THEN 'Monitor performance'
        ELSE 'High revenue - expand operations'
    END AS action

FROM facts f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.area_name
ORDER BY revenue DESC;

--Rule-based recommendation for AOV
SELECT 
    AVG(total_amount) AS AOV,

    CASE 
        WHEN AVG(total_amount) < 200 
            THEN 'Introduce combo offers / upselling'
        WHEN AVG(total_amount) BETWEEN 200 AND 500 
            THEN 'Maintain strategy'
        ELSE 'Target premium customers'
    END AS action

FROM facts;


--DECISION SYSTEM

--Rule-based recommendation for average delivery time

SELECT 
    AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) AS avg_delivery_time,

    CASE 
        WHEN AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) > 45 
            THEN 'Critical - Improve delivery operations'
        WHEN AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) > 35 
            THEN 'Moderate delay - monitor'
        ELSE 'Efficient delivery'
    END AS action

FROM facts f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.online_order = 'Yes';


--Rule-based recommendation for average delivery time of each area

SELECT 
    l.area_name,
    AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) AS avg_delivery_time,

    CASE 
        WHEN AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) > 45 
            THEN 'Increase delivery partners in this zone'
        WHEN AVG(DATEDIFF(MINUTE, d.order_time, f.delivery_time)) > 35 
            THEN 'Optimize routing'
        ELSE 'No action needed'
    END AS action

FROM facts f
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY l.area_name
ORDER BY avg_delivery_time DESC;

--Rule-based recommendation for delivery success rate

SELECT 
    COUNT(delivery_time) * 100.0 / COUNT(*) AS success_rate,

    CASE 
        WHEN COUNT(delivery_time) * 100.0 / COUNT(*) < 85 
            THEN 'Critical - Investigate failures'
        ELSE 'Stable system'
    END AS action

FROM facts
WHERE online_order = 'Yes';

--DECISION SYSTEM

--Rule-based recommendation for peak hours

SELECT 
    DATEPART(HOUR, d.order_time) AS hour_of_day,
    COUNT(*) AS total_orders,

    CASE 
        WHEN COUNT(*) > 500 
            THEN 'Increase delivery fleet'
        WHEN COUNT(*) > 300 
            THEN 'Moderate capacity required'
        ELSE 'Normal'
    END AS action

FROM facts f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY DATEPART(HOUR, d.order_time)
ORDER BY total_orders DESC;

--Rule-based recommendation for total orders per area
SELECT 
    l.area_name,
    COUNT(*) AS total_orders,

    CASE 
        WHEN COUNT(*) > 300 
            THEN 'Expand delivery network'
        WHEN COUNT(*) > 250 
            THEN 'Monitor demand'
        ELSE 'Low demand area'
    END AS action

FROM facts f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.area_name
ORDER BY total_orders DESC;

--Rule-based recommendation for table booking
SELECT 
    CASE 
        WHEN 
        (SELECT AVG(total_amount) FROM facts WHERE book_table='Yes') >
        (SELECT AVG(total_amount) FROM facts WHERE book_table='No')
        THEN 'Promote table booking feature'
        ELSE 'No strong impact'
    END AS action;


--DECISION SYSTEM

--Rule-based recommendation for ratings

SELECT 
    r.restaurant_name,
    COUNT(*) AS total_orders,
    AVG(f.rating_numeric) AS avg_rating,

    CASE 
        WHEN AVG(f.rating_numeric) < 3 AND COUNT(*) > 20 
            THEN 'High Risk - Review or remove'
        WHEN AVG(f.rating_numeric) < 3.5 
            THEN 'Needs improvement'
        ELSE 'Good performance'
    END AS action

FROM facts f
JOIN dim_restaurant r ON f.restaurant_key = r.restaurant_key
GROUP BY r.restaurant_name
ORDER BY avg_rating;

--Rule-based recommendation for customer dissatisfication

SELECT 
    f.order_id,
    f.rating_numeric,
    DATEDIFF(MINUTE, d.order_time, f.delivery_time) AS delivery_time,

    CASE 
        WHEN f.rating_numeric <= 2 
             OR DATEDIFF(MINUTE, d.order_time, f.delivery_time) > 45 
            THEN 'Offer compensation / investigate issue'
        ELSE 'No issue'
    END AS action

FROM facts f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.delivery_time IS NOT NULL AND f.rating_numeric IS NOT NULL
ORDER BY f.rating_numeric,f.delivery_time;


