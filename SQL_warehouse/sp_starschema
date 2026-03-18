CREATE OR ALTER PROCEDURE dbo.sp_silver
AS
BEGIN
    
    IF OBJECT_ID('dbo.facts', 'U') IS NOT NULL DROP TABLE dbo.facts;
    IF OBJECT_ID('dbo.dim_restaurant', 'U') IS NOT NULL DROP TABLE dbo.dim_restaurant;
    IF OBJECT_ID('dbo.dim_location', 'U') IS NOT NULL DROP TABLE dbo.dim_location;
    IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;
    --dimensions

    --dim_restaurant
    CREATE TABLE dim_restaurant AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY restaurant) AS restaurant_key,
        restaurant AS restaurant_name,
        cuisine AS cuisine_type
    FROM
    (   SELECT DISTINCT restaurant,cuisine
        FROM myadls.dbo.silver_orders
    ) AS restaurant_data;


    --dim_location
    CREATE TABLE dim_location AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY location) AS location_key,
        location AS area_name,
        city
    FROM 
    (   SELECT DISTINCT location,city
        FROM myadls.dbo.silver_orders
    ) AS location_data;

    --dim_date
    CREATE TABLE dim_date AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY order_time) AS date_key,
        order_time,
        DATEPART(HOUR, order_time) AS order_hour,
        DATEPART(DAY, order_time) AS order_day,
        DATEPART(MONTH, order_time) AS order_month,
        DATEPART(YEAR, order_time) AS order_year
    FROM (
        SELECT DISTINCT order_time
        FROM myadls.dbo.silver_orders
    ) AS date_data;

    --DROP TABLE facts
    --Facts
    CREATE TABLE facts AS
    SELECT
        s.order_id,
        r.restaurant_key,
        l.location_key,
        d.date_key,
        s.total_amount,
        s.ordered_qty,
        s.delivery_time,
        s.distance_km,
        s.rating_numeric,
        s.book_table,
        s.online_order
    FROM
    myadls.dbo.silver_orders s JOIN dim_restaurant r
    ON s.restaurant=r.restaurant_name AND s.cuisine=r.cuisine_type
    JOIN dim_location l
    ON s.location=l.area_name
    JOIN dim_date d
    ON s.order_time=d.order_time;

END

SELECT COUNT(*) FROM myadls.dbo.silver_orders
SELECT COUNT(*) FROM facts

SELECT COUNT(*) FROM dim_restaurant; --195

SELECT COUNT(*) FROM dim_location; --15

SELECT COUNT(*) FROM dim_date; --3946


--data quality checks
SELECT restaurant_name, COUNT(*)
FROM dim_restaurant
GROUP BY restaurant_name
HAVING COUNT(*) > 1;


