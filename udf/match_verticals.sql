CREATE OR REPLACE FUNCTION `%s.%s`.udf_match_verticals(cohort_level STRING, cohort_name STRING) AS (
    cohort_level IN (
        'Vertical',
        'Feature'
    ) AND
    cohort_name IN (
        'Browser',
        'Game',
        'Lifestyle',
        'Shopping',
        'Travel',
        'feature: visit_shopping_content_tab',
        'feature: visit_game_content_tab',
        'feature: visit_lifestyle_content_tab',
        'feature: visit_travel_content_tab',
        'feature: tab_swipe'
    )
);
