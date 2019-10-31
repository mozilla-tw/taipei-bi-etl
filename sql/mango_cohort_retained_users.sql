SELECT os, country,
       measure_type, cohort_level, cohort_name, 
       cohort_date,

       COUNT(DISTINCT CASE WHEN occur_day = 0 THEN client_id ELSE NULL END) AS daily_cohort_size,
       COUNT(DISTINCT CASE WHEN occur_day = 1 THEN client_id ELSE NULL END) AS d1_retained_users,
       COUNT(DISTINCT CASE WHEN occur_day = 7 THEN client_id ELSE NULL END) AS d7_retained_users,
       COUNT(DISTINCT CASE WHEN occur_day = 14 THEN client_id ELSE NULL END) AS d14_retained_users,
       COUNT(DISTINCT CASE WHEN occur_day = 28 THEN client_id ELSE NULL END) AS d28_retained_users,
       COUNT(DISTINCT CASE WHEN occur_day = 56 THEN client_id ELSE NULL END) AS d56_retained_users,
       COUNT(DISTINCT CASE WHEN occur_day = 84 THEN client_id ELSE NULL END) AS d84_retained_users,

       COUNT(DISTINCT CASE WHEN occur_week = 0 THEN client_id ELSE NULL END) AS weekly_cohort_size,
       COUNT(DISTINCT CASE WHEN occur_week = 1 THEN client_id ELSE NULL END) AS w1_retained_users,
       COUNT(DISTINCT CASE WHEN occur_week = 2 THEN client_id ELSE NULL END) AS w2_retained_users,
       COUNT(DISTINCT CASE WHEN occur_week = 3 THEN client_id ELSE NULL END) AS w3_retained_users,
       COUNT(DISTINCT CASE WHEN occur_week = 4 THEN client_id ELSE NULL END) AS w4_retained_users,
       COUNT(DISTINCT CASE WHEN occur_week = 8 THEN client_id ELSE NULL END) AS w8_retained_users,
       COUNT(DISTINCT CASE WHEN occur_week = 12 THEN client_id ELSE NULL END) AS w12_retained_users,

       COUNT(DISTINCT CASE WHEN occur_month = 0 THEN client_id ELSE NULL END) AS monthly_cohort_size,
       COUNT(DISTINCT CASE WHEN occur_month = 1 THEN client_id ELSE NULL END) AS m1_retained_users,
       COUNT(DISTINCT CASE WHEN occur_month = 2 THEN client_id ELSE NULL END) AS m2_retained_users,
       COUNT(DISTINCT CASE WHEN occur_month = 3 THEN client_id ELSE NULL END) AS m3_retained_users

FROM `{project}.{dataset}.{src}`

GROUP BY os, country,
       measure_type, cohort_level, cohort_name, 
       cohort_date
