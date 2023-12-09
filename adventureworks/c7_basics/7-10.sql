USE tpch;

-- 35,1.12903226,0.09408602166666667,2023-12-10
SELECT
    datediff('2022-11-05', '2022-10-01') diff_in_days,
    months_between('2022-11-05', '2022-10-01') diff_in_months,
    months_between('2022-11-05', '2022-10-01') / 12 diff_in_years,
    date_add('2022-11-05', 10) AS new_date;