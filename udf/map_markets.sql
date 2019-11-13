CREATE OR REPLACE FUNCTION `%s.%s`.udf_map_markets(country STRING) AS (CASE
                                                             WHEN country IN ('IN',
                                                                              'ID') THEN country
                                                             WHEN country IN ('TW',
                                                                              'HK',
                                                                              'SG') THEN 'THS'
                                                             WHEN country IN ('PH',
                                                                              'VN',
                                                                              'TH',
                                                                              'MY') THEN 'PVTM'
                                                             WHEN country IN ('BD',
                                                                              'MM',
                                                                              'KH',
                                                                              'LA',
                                                                              'BN') THEN 'BMKLB'
                                                             WHEN country IS NULL THEN 'Unknown'
                                                             ELSE 'Others'
                                                         END);
