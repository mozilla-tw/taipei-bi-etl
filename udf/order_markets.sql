CREATE OR REPLACE FUNCTION `%s.%s`.udf_order_markets(country STRING) AS (
    CASE
        WHEN country = 'Overall' THEN 0
        WHEN country ='IN' THEN 1
        WHEN country ='ID' THEN 2
        WHEN country ='THS' THEN 3
        WHEN country ='PVTM' THEN 4
        WHEN country ='BMKLB' THEN 5
        WHEN country ='Unknown' THEN 99
        WHEN country ='Others' THEN 100
        ELSE 9999
    END
);
