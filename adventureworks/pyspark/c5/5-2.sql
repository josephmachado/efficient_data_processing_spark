USE tpch;

SELECT
    orderkey,
    linenumber,
    extendedprice,
    ROUND(
        sum(extendedprice) over(
            PARTITION by orderkey
            ORDER BY
                linenumber
        ),
        2
    ) AS total_extendedprice
FROM
    lineitem
ORDER BY
    orderkey,
    linenumber
LIMIT
    20;