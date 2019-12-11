CREATE OR REPLACE FUNCTION `%s.%s`.order_verticals(feature STRING) AS (
    CASE
        WHEN feature = 'vertical: browser' THEN 1
        WHEN feature = 'vertical: shopping' THEN 2
        WHEN feature = 'vertical: game' THEN 3
        WHEN feature = 'vertical: lifestyle' THEN 4
        WHEN feature = 'vertical: travel' THEN 5
        WHEN feature = 'shopping: shopping tab_swipe' THEN 11
        WHEN feature = 'shopping: shopping content_tab' THEN 12
        WHEN feature = 'game: game content_tab' THEN 13
        WHEN feature = 'lifestyle: lifestyle content_tab' THEN 14
        WHEN feature = 'travel: travel content_tab' THEN 15
        ELSE 9999
    END
);
