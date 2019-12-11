CREATE OR REPLACE FUNCTION `%s.%s`.udf_order_channels(channel STRING) AS (
CASE
    WHEN channel = 'Overall' THEN 0
    WHEN channel = 'Earned Media' THEN 1
    WHEN channel = 'Owned Media' THEN 2
    WHEN channel = 'Paid Media' THEN 3
    WHEN channel = 'Distribution' THEN 4
    WHEN channel = 'Referral' THEN 5
    WHEN channel = 'Unknown' THEN 99
    WHEN channel = 'Others' THEN 100
    ELSE 9999
END
);
