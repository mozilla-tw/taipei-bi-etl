CREATE OR REPLACE FUNCTION `%s.%s`.udf_order_target_countries(country STRING) AS (
    CASE
        WHEN country ='IN' THEN '01'
        WHEN country ='ID' THEN '02'
        WHEN country ='TW' THEN '03'
        WHEN country ='HK' THEN '04'
        WHEN country ='SG' THEN '05'
        WHEN country ='PH' THEN '06'
        WHEN country ='VN' THEN '07'
        WHEN country ='TH' THEN '08'
        WHEN country ='MY' THEN '09'
        WHEN country ='BD' THEN '10'
        WHEN country ='MM' THEN '11'
        WHEN country ='KH' THEN '12'
        WHEN country ='LA' THEN '13'
        WHEN country ='BN' THEN '14'
        WHEN country = '' OR country IS NULL THEN '~'
        ELSE country
    END
);
