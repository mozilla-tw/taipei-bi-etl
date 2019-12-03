CREATE OR REPLACE FUNCTION `%s.%s`.udf_js_feature_mapping (
    event_method STRING,
    event_object STRING,
    event_value STRING,
    extra_key STRING,
    extra_value STRING,
    event_vertical STRING,
    settings_search_engine STRING
)

RETURNS STRUCT<
  feature ARRAY<STRING>,
  vertical STRING,
  app STRING
>

LANGUAGE js
AS """

var partner_list = ['bukalapak', 'flipkart',
                      'liputan6', 'gameloft',
                      'atmegame', 'gamezop', 'frvr',
                      'booking.com',
                      'dailyhunt',
                      'google'];


    // vertical = Browser
    // app = App
    function do_browser() {
        var feature = [];
        var vertical = '';
        var app = '';

        if (event_method == 'add' &&
            event_object == 'tab' &&
            ['toolbar','tab_tray'].includes(event_value)
        ) {
          feature.push('feature: add_tab');
        }

        if (event_method == 'change' &&
            event_object == 'tab'
        ) {
          feature.push('feature: change_tab');
        }

        if (event_method == 'click' &&
            event_object == 'close_all' &&
            event_value == 'tab_tray'
        ) {
          feature.push('feature: close_all_tab');
        }

        if (['remove', 'swipe'].includes(event_method) &&
            event_object == 'tab' &&
            event_value == 'tab_tray'
        ) {
          feature.push('feature: remove_tab');
        }

        if (event_value == 'block_image'
        ) {
          feature.push('feature: change_block_image');
        }

        if (event_method != 'share' &&
            event_value == 'bookmark'
        ) {
          feature.push('feature: bookmark');
        }

        if ((
            ['click', 'show'].includes(event_method) &&
            event_value == 'history'
           ) || (
            event_method == 'open' &&
            event_object == 'panel' &&
            event_value == 'link'
        )) {
          feature.push('feature: visit_history');
        }

        if ((
             // clear all
             event_method == 'clear' &&
             event_object == 'panel' &&
             event_value == 'history'
           ) || (
             // remove one
             event_method == 'remove' &&
             event_object == 'panel' &&
             event_value == 'link'
        )) {
          feature.push('feature: clean_history');
        }

        if (event_value == 'clear_cache'
        ) {
          feature.push('feature: clear_cache');
        }

        if ((
              ['change', 'click'].includes(event_method) &&
              event_object == 'default_browser'
        ) || (
              ['change', 'click'].includes(event_method) &&
              event_value.includes('default_browser')
        )) {
          feature.push('feature: change_default_browser');
        }

        if (['click', 'change'].includes(event_method) &&
            event_value.includes('save_downloads_to')
        ) {
          feature.push('feature: settings_change_download_location');
        }

        if (event_value.includes('clear_browsing_data')
        ) {
          feature.push('feature: settings_clear_browsing_data');
        }

        if (event_value == 'pref_locale'
        ) {
          feature.push('feature: settings_change_locale');
        }

        if (event_object == 'setting' &&
            event_value == 'telemetry'
        ) {
          feature.push('feature: settings_change_collection_telemetry');
        }

        if (event_method == 'click' &&
            event_object == 'menu' &&
            event_value == 'settings'
        ) {
          feature.push('feature: visit_settings');
        }

        if (event_value == 'download' ||
            (
              event_method == 'open' &&
              event_object == 'panel' &&
              event_value == 'file'
        )) {
          feature.push('feature: visit_download');
        }

        if (['remove', 'delete'].includes(event_method) &&
            event_object == 'panel' &&
            event_value == 'file'
        ) {
          feature.push('feature: clean_download_file');
        }

        if (event_method == 'click' &&
            event_object == 'menu' &&
            event_value == 'exit'
        ) {
          feature.push('feature: exit');
        }

        if (event_method == 'click' &&
            (
              event_object == 'feedback' ||
              event_value.includes('feedback')
        )) {
          feature.push('feature: give_feedback');
        }

        if (event_object == 'find_in_page' ||
            event_value == 'find_in_page'
        ) {
          feature.push('feature: find_in_page');
        }

        if (event_value == 'forward'
        ) {
          feature.push('feature: forward_page');
        }

        if (event_value == 'fullscreen'
        ) {
          feature.push('feature: fullscreen');
        }

        if (event_object == 'landscape_mode'
        ) {
          feature.push('feature: landscape_mode');
        }

        if (event_method == 'open' &&
            event_object == 'home' &&
            event_value == 'link'
        ) {
          feature.push('feature: visit_topsite');
        }

        if (event_method == 'open' &&
            event_object == 'home' &&
            event_value == 'link' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('source: '.concat(extra_value));
          feature.push('visit_topsite_partner: true');
        }

        if (event_method == 'remove' &&
            event_object == 'home' &&
            event_value == 'link'
        ) {
          feature.push('feature: remove_topsite');
        }

        if (event_method == 'change' &&
            event_value.includes('night_mode')
        ) {
          feature.push('feature: change_night_mode');
        }

        if (event_method == 'pin_shortcut'
        ) {
          feature.push('feature: pin_shortcut');
        }

        if ((event_method != 'show' &&
             event_object.includes('private_')
            ) || (

            !['show', 'launch'].includes(event_method) &&
             event_value.includes('private_')
        )) {
          feature.push('feature: private_mode');
        }

        if (event_value == 'reload_page'
        ) {
          feature.push('feature: reload_page');
        }

        if (event_method != 'share' &&
            (
              event_object == 'capture' ||
              event_value == 'capture'
            )
        ) {
          feature.push('feature: screenshot');
        }

        if (event_object == 'browser_contextmenu' ||
              (
                event_method == 'long_press' &&
                event_object == 'browser'
              )
        ) {
          feature.push('feature: browse');
        }

        if ((['show', 'cancel', 'clear'].includes(event_method) &&
             event_object == 'search_bar'
            ) || (
             event_method == 'long_press' &&
             event_object == 'search_suggestion'
        )) {
          feature.push('feature: pre_search');
        }

        if ((['type_query', 'select_query'].includes(event_method) &&
             event_object == 'search_bar'
            ) || (
             event_method == 'click' &&
             event_object == 'quicksearch'
            ) || (
             event_method == 'open' &&
             event_object == 'search_bar' &&
             event_value == 'link'
        )) {
          feature.push('feature: search');
        }

        if (['type_query', 'select_query'].includes(event_method) &&
            event_object == 'search_bar' &&
			['google',''].includes(settings_search_engine) // null as default
        ) {
          feature.push('source: google');
          feature.push('feed: google');
          feature.push('search_partner: true');
        }

        if (['type_query', 'select_query'].includes(event_method) &&
            event_object == 'search_bar'
        ) {
          feature.push('tags: keyword_search');
        }

        if (event_method == 'click' &&
            event_object == 'quicksearch'
        ) {
          feature.push('tags: quicksearch');
        }

        if (event_method == 'click' &&
            event_object == 'quicksearch' &&
            extra_key == 'engine' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('source: '.concat(extra_value));
          feature.push('quicksearch_partner: true');
        }

        if (event_method == 'open' &&
            event_object == 'search_bar' &&
            extra_key == 'link'
        ) {
          feature.push('tags: url_search');
        }

        if (['change', 'click'].includes(event_method) &&
            event_object == 'setting' &&
            event_value == 'search_engine'
        ) {
          feature.push('feature: settings_change_search_engine');
        }

        if (event_method == 'share' ||
            (
              event_object == 'setting' &&
              event_value.includes('share_with_friends')
            )
        ) {
          feature.push('feature: share');
        }

        if (event_object == 'themetoy'
        ) {
          feature.push('feature: themetoy');
        }

        if (event_method == 'change' &&
            event_value.includes('turbo')
        ) {
          feature.push('feature: change_turbo_mode');
        }

        if ((event_method == 'click' &&
             event_object.includes('vpn') &&
             event_value == 'positive'
            ) || (
             event_method == 'click' &&
             event_value.includes('vpn')
        )) {
          feature.push('feature: vpn');
        }

        if (event_method == 'click' &&
            event_object == 'setting' &&
            event_value == 'learn_more'
        ) {
          feature.push('feature: settings_learn_more');
        }

        if (event_method == 'launch' &&
            event_object == 'app'
        ) {
          feature.push('feature: launch_app');
        }

        if (event_method == 'launch' &&
            event_object == 'app' &&
            event_value == 'external_app'
        ) {
          feature.push('tags: launch_app_from_external');
        }

        if (event_method == 'launch' &&
            event_object == 'app' &&
            event_value == 'launcher'
        ) {
          feature.push('tags: launch_app_from_launcher');
        }

        if (event_method == 'launch' &&
            event_object == 'app' &&
            ['shortcut', 'private_mode', 'game_shortcut'].includes(event_value)
        ) {
          feature.push('tags: launch_app_from_shortcut');
        }

        if (event_vertical == 'all'
        ) {
          feature.push('tags: browser_vertical');
        }

        if (feature.length > 0
        ) {
          vertical = 'Browser';
          app = 'App';
        }
        return [feature, vertical, app];
    }


    // vertical = Shopping
    // app = App
    function do_shopping() {
        var feature = [];
        var vertical = '';
        var app = '';

        // lifefeed
        if (event_value == 'lifefeed_ec'
        ) {
          feature.push('feature: lifefeed');
          feature.push('category: e_ticket');
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_ec' &&
            extra_key == 'category'
        ) {
          feature.push('component_type_id: 9'); // icon_card
          feature.push('tags: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_ec' &&
            extra_key == 'source'
        ) {
          feature.push('component_type_id: 9'); // icon_card
          feature.push('feed: '.concat(extra_value));
          feature.push('source: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_ec' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('lifefeed_ec_partner: true');
        }

        if (event_value == 'lifefeed_promo'
        ) {
          feature.push('feature: lifefeed');
          feature.push('category: coupon');
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_promo' &&
            extra_key == 'feed' &&
            extra_value == 'list'
        ) {
          feature.push('component_type_id: 7'); // list
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_promo' &&
            extra_key == 'feed' &&
            extra_value == 'banner'
        ) {
          feature.push('component_type_id: 6'); // banner
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_promo' &&
            extra_key == 'source'
        ) {
          feature.push('feed: '.concat(extra_value));
          feature.push('source: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_promo' &&
            extra_key == 'subcategory'
        ) {
          feature.push('tags: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_value == 'lifefeed_promo' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('lifefeed_promo_partner: true');
        }

        // tab_swipe
        if (// enter & leave
            (
              ['click', 'start', 'end', 'clear'].includes(event_method) &&
              (
                event_value.includes('tab_swipe') ||
                event_object == 'tab_swipe'
              ) &&
              event_vertical == 'shopping'
            )
        ) {
          feature.push('feature: tab_swipe');
        }

        if (event_method == 'end' &&
            event_object == 'tab_swipe' &&
            extra_key == 'feed'
        ) {
          feature.push('feed: '.concat(extra_value));
        }

        if (event_method == 'end' &&
            event_object == 'tab_swipe' &&
            extra_key == 'source'
        ) {
          feature.push('source: '.concat(extra_value));
        }

        if (event_method == 'end' &&
            event_object == 'tab_swipe' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('tab_swipe_partner: true');
        }

        if (event_method == 'change' &&
            event_object == 'setting' &&
            event_value == 'tab_swipe'
        ) {
          feature.push('tags: change_tab_swipe_settings');
        }

        // content_hub
        if (event_object == 'content_hub' &&
            event_vertical == 'shopping'
        ) {
          feature.push('feature: visit_shopping_content_hub');
        }

        // category
        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'shopping'
        ) {
          feature.push('feature: open_category_shopping');
        }

        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'shopping' &&
            extra_key == 'category'
        ) {
          feature.push('tags: open_category_shopping_'.concat(extra_value));
        }

        // content_tab
        if (event_object == 'content_tab' &&
            event_vertical == 'shopping'
        ) {
          feature.push('feature: visit_shopping_content_tab');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'feed'
        ) {
          feature.push('feed: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'source'
        ) {
          feature.push('source: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('visit_shopping_content_tab_partner: true');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'category'
        ) {
          feature.push('shopping_content_tab_category: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'subcategory_id'
        ) {
          feature.push('shopping_content_tab_subcategory_id: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'shopping' &&
            extra_key == 'component_id'
        ) {
          feature.push('shopping_content_tab_component_id: '.concat(extra_value));
        }

        if (event_vertical == 'shopping'
        ) {
          feature.push('tags: shopping_vertical');
        }

        if (feature.length > 0
        ) {
          vertical = 'Shopping';
          app = 'App';
        }
        return [feature, vertical, app];
    }

    // vertical = Lifestyle
    // app = App
    function do_lifestyle() {
        var feature = [];
        var vertical = '';
        var app = '';

        //lifefeed
        if (event_value == 'lifefeed_news'
        ) {
          feature.push('feature: lifefeed_news');
        }

        if (event_method == 'open' &&
            event_value == 'lifefeed_news' &&
            extra_key == 'category'
        ) {
          feature.push('category: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_object == 'panel' &&
            event_value == 'lifefeed_news' &&
            extra_key == 'feed'
        ) {
          feature.push('component_type_id: 7'); // list
          feature.push('feed: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_object == 'panel' &&
            event_value == 'lifefeed_news' &&
            extra_key == 'source'
        ) {
          feature.push('component_type_id: 7'); // list
          feature.push('source: '.concat(extra_value));
        }

        if (event_method == 'click' &&
            event_object == 'panel' &&
            event_value == 'lifefeed_news' &&
            extra_key == 'feed' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('lifefeed_news_partner: true');
        }

        // content_hub
        if (event_object == 'content_hub' &&
            event_vertical == 'lifestyle'
        ) {
          feature.push('feature: visit_lifestyle_content_hub');
        }

        // category
        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'lifestyle'
        ) {
          feature.push('feature: open_category_lifestyle');
        }

        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'category'
        ) {
          feature.push('tags: open_category_lifestyle_'.concat(extra_value));
        }

        // content_tab
        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle'
        ) {
          feature.push('feature: visit_lifestyle_content_tab');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'feed'
        ) {
          feature.push('feed: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'source'
        ) {
          feature.push('source: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('visit_lifestyle_content_tab_partner: true');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'category'
        ) {
          feature.push('lifestyle_content_tab_category: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'subcategory_id'
        ) {
          feature.push('lifestyle_content_tab_subcategory_id: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'lifestyle' &&
            extra_key == 'component_id'
        ) {
          feature.push('lifestyle_content_tab_component_id: '.concat(extra_value));
        }

        if (event_vertical == 'lifestyle'
        ) {
          feature.push('tags: lifestyle_vertical');
        }

        if (feature.length > 0
        ) {
          vertical = 'Lifestyle';
          app = 'App';
        }
        return [feature, vertical, app];
    }

    // vertical = Game
    // app = App
    function do_game() {
        var feature = [];
        var vertical = '';
        var app = '';

        // content_hub
        if (event_object == 'content_hub' &&
            event_vertical == 'game'
        ) {
          feature.push('feature: visit_game_content_hub');
        }

        // category
        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'game'
        ) {
          feature.push('feature: open_category_game');
        }

        if (event_method == 'open' &&
            event_object == 'category' &&
            event_vertical == 'game' &&
            extra_key == 'category'
        ) {
          feature.push('tags: open_category_game_'.concat(extra_value));
        }

        // content_tab
        if (event_object == 'content_tab' &&
            event_vertical == 'game'
        ) {
          feature.push('feature: visit_game_content_tab');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'feed'
        ) {
          feature.push('feed: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'source'
        ) {
          feature.push('source: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'source' &&
            partner_list.includes(extra_value)
        ) {
          feature.push('visit_game_content_tab_partner: true');
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'category'
        ) {
          feature.push('game_content_tab_category: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'subcategory_id'
        ) {
          feature.push('game_content_tab_subcategory_id: '.concat(extra_value));
        }

        if (event_object == 'content_tab' &&
            event_vertical == 'game' &&
            extra_key == 'component_id'
        ) {
          feature.push('game_content_tab_component_id: '.concat(extra_value));
        }

        if (event_vertical == 'game'
        ) {
          feature.push('tags: game_vertical');
        }

        if (feature.length > 0
        ) {
          vertical = 'Game';
          app = 'App';
        }
        return [feature, vertical, app];
    }

    // vertical = Travel
    // app = App
    function do_travel() {
        var feature = [];
        var vertical = '';
        var app = '';

        if (event_vertical == 'travel'
        ) {
          feature.push('tags: travel_vertical');
        }

        if (feature.length > 0
        ) {
          vertical = 'Travel';
          app = 'App';
        }
        return [feature, vertical, app];
    }

    // vertical = Others
    // app = Others
    function do_others() {
        return [['feature: others'], 'Others', 'Others'];
    }

    function run_mapping() {
      var browser = do_browser();
      var shopping = do_shopping();
      var lifestyle = do_lifestyle();
      var game = do_game();
      var travel = do_travel();
      var others = do_others();

      var checker_do_next = function(x){ return (x[0].length == 0) && (x[1] == '') && (x[2] == '') };
      var result = [];

      if(!checker_do_next(browser)) {
        result = browser;
      } else if(!checker_do_next(shopping)) {
        result = shopping;
      } else if(!checker_do_next(lifestyle)) {
        result = lifestyle;
      } else if(!checker_do_next(game)) {
        result = game;
      } else if(!checker_do_next(travel)) {
        result = travel;
      } else {
        result = others;
      }
      return {
        'feature': result[0],
        'vertical': result[1],
        'app': result[2]
      };
    }

    return run_mapping();

""";