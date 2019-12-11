CREATE OR REPLACE FUNCTION `%s.%s`.udf_map_verticals(cohort_name STRING) AS (
    CASE
        WHEN cohort_name = 'Browser' THEN 'vertical: browser'
        WHEN cohort_name = 'Shopping' THEN 'vertical: shopping'
        WHEN cohort_name = 'Game' THEN 'vertical: game'
        WHEN cohort_name = 'Lifestyle' THEN 'vertical: lifestyle'
        WHEN cohort_name = 'Travel' THEN 'vertical: travel'
        WHEN cohort_name = 'feature: tab_swipe' THEN 'shopping: shopping tab_swipe'
        WHEN cohort_name = 'feature: visit_shopping_content_tab' THEN 'shopping: shopping content_tab'
        WHEN cohort_name = 'feature: visit_game_content_tab' THEN 'game: game content_tab'
        WHEN cohort_name = 'feature: visit_lifestyle_content_tab' THEN 'lifestyle: lifestyle content_tab'
        WHEN cohort_name = 'feature: visit_travel_content_tab' THEN 'travel: travel content_tab'
        ELSE ''
    END
);
