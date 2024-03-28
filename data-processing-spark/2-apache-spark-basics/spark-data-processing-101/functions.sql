SELECT LENGTH('hi');

USE tpch;

SELECT CONCAT(clerk, '-', orderpriority) FROM ORDERS LIMIT 5;

SELECT SPLIT(clerk, '#') FROM ORDERS LIMIT 5;

SELECT clerk, SUBSTR(clerk, 1, 5) FROM orders LIMIT 5;

SELECT TRIM(' hi ');

-- 35,1.12903226,0.09408602166666667,2023-12-10
SELECT
    datediff('2022-11-05', '2022-10-01') diff_in_days,
    months_between('2022-11-05', '2022-10-01') diff_in_months,
    months_between('2022-11-05', '2022-10-01') / 12 diff_in_years,
    date_add('2022-11-05', 10) AS new_date;

SELECT DATE '2022-11-05' + INTERVAL '10' DAY;

SELECT TO_DATE('11-05-2023', 'MM-dd-yyyy') AS parsed_date, to_timestamp('11-05-2023', 'MM-dd-yyyy') as parsed_ts;

SELECT date_format(orderdate, 'yyyy-MM-01') AS first_month_date FROM orders LIMIT 5; 

SELECT year(date '2023-11-05');

SELECT ROUND(100.102345, 2);

SELECT ABS(-100), ABS(100);

SELECT CEIL(100.1), FLOOR(100.1);