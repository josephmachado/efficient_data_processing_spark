use tpch;

-- calculating the totalprice of an order (with orderkey = 1) from it's individual items
SELECT
    orderkey,
    round( sum(extendedprice * (1 - discount) * (1 + tax)),
        2
    ) AS totalprice
    -- Formula to calculate price paid after discount & tax
FROM
    lineitem
WHERE
    orderkey = 1
GROUP BY
    orderkey;

/*
 orderkey | totalprice
----------+------------
        1 |  173665.51
*/

-- The totalprice of an order (with orderkey = 1)
SELECT
    orderkey,
    totalprice
FROM
    orders
WHERE
    orderkey = 1;

/*
 orderkey | totalprice
----------+------------
        1 |  173665.47
*/

