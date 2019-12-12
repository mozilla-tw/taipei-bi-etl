CREATE OR REPLACE FUNCTION `%s.%s`.udf_order_channel_levels(channel STRING) AS (
CASE
    WHEN channel = 'appstore' THEN 0
    WHEN channel = 'network' THEN 1
    WHEN channel = 'campaign' THEN 2
    WHEN channel = 'adgroup' THEN 3
    WHEN channel = 'creative' THEN 4
    ELSE 9999
END
);
