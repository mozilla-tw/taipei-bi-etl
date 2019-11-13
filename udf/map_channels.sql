CREATE OR REPLACE FUNCTION `%s.%s`.udf_map_channels(cohort_name STRING) AS (CASE
                                                                 WHEN cohort_name IN ('Organic',
                                                                                      'Google Organic Search',
                                                                                      'New Release',
                                                                                      '2019 TW launch NR',
                                                                                      'Marcom') THEN 'Earned Media'
                                                                 WHEN cohort_name IN ('Product Marketing (Owned media)') THEN 'Owned Media'
                                                                 WHEN cohort_name IN ('Adwords UAC Installs',
                                                                                      'Twitter Installs',
                                                                                      'Twitter Audience Platform',
                                                                                      '2019quiz_campaign',
                                                                                      'Facebook Installs',
                                                                                      'Off-Facebook Installs',
                                                                                      'Appier',
                                                                                      'FlashAds') THEN 'Paid Media'
                                                                 WHEN cohort_name IN ('GMobi',
                                                                                      'CherryMobile',
                                                                                      'MoMagic',
                                                                                      'ARVE') THEN 'Distribution'
                                                                 WHEN cohort_name IN ('InviteReferrals') THEN 'Referral'
                                                                 WHEN cohort_name IS NULL THEN 'Unknown'
                                                                 ELSE 'Others'
                                                             END);
