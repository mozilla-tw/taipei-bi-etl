"""Filter events and label with features."""
from pandas import Series


partner_list = ['bukalapak', 'flipkart', 'jd.id', 'gamezop']


class FirefoxLiteApp:

    class BrowserVertical:
        """Browser Vertical."""

        @staticmethod
        def add_tab_feature(e: Series):
            if (e['event_method'] == 'add' and
               e['event_object'] == 'tab'):
                return ['feature: add_tab']
            return False

        @staticmethod
        def change_tab_feature(e: Series):
            if (
                e['event_method'] == 'change' and
                e['event_object'] == 'tab'
            ):
                return ['feature: change_tab']
            return False

        @staticmethod
        def close_all_tab_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'close_all' and
                e['event_value'] == 'tab_tray'
            ):
                return ['feature: close_all_tab']
            return False

        @staticmethod
        def remove_tab_feature(e: Series):
            if (
                e['event_method'] in ['remove', 'swipe'] and
                e['event_object'] == 'tab' and
                e['event_value'] in ['tab_tray','tab_swipe']
            ):
                return ['feature: remove_tab']
            return False

        @staticmethod
        def remove_tab_feature(e: Series):
            if (
                e['event_method'] in ['remove', 'swipe'] and
                e['event_object'] == 'tab' and
                e['event_value'] == 'tab_tray'
            ):
                return ['feature: remove_tab']
            return False

        @staticmethod
        def change_block_image_feature(e: Series):
            if (
                e['event_value'] == 'block_image'
            ):
                return ['feature: change_block_image']
            return False

        @staticmethod
        def bookmark_feature(e: Series):
            if (
                e['event_method'] != 'share' and
                e['event_value'] == 'bookmark'
            ):
                return ['feature: bookmark']
            return False

        @staticmethod
        def visit_history_feature(e: Series):
            if ((
                e['event_method'] in ['click', 'show'] and
                e['event_value'] == 'history'
            ) or (
                e['event_method'] == 'open' and
                e['event_object'] == 'panel' and
                e['event_value'] == 'link'
            )):
                return ['feature: visit_history']
            return False

        @staticmethod
        def clean_history_feature(e: Series):
            if ((
                    # clear all
                    e['event_method'] == 'clear' and
                    e['event_object'] == 'panel' and
                    e['event_value'] == 'history'
                ) or (
                    # remove one
                    e['event_method'] == 'remove' and
                    e['event_object'] == 'panel' and
                    e['event_value'] == 'link'
            )):
                return ['feature: clean_history']
            return False

        @staticmethod
        def clear_cache_feature(e: Series):
            if (
                e['event_value'] == 'clear_cache'
            ):
                return ['feature: clear_cache']
            return False

        @staticmethod
        def change_default_browser_feature(e: Series):
            if ((
                e['event_method'] in ['change','click'] and
                e['event_object'] == 'default_browser'
            ) or
                (
                e['event_method'] in ['change','click'] and
                e['event_value'] is not None and
                'default_browser' in e['event_value']
            )):
                return ['feature: change_default_browser']
            return False

        @staticmethod
        def settings_change_download_location_feature(e: Series):
            if (
                e['event_method'] in ['click','change'] and
                e['event_value'] is not None and
                'save_downloads_to' in e['event_value']
            ):
                return ['feature: settings_change_download_location']
            return False

        @staticmethod
        def settings_clear_browsing_data_feature(e: Series):
            if (
                e['event_value'] is not None and
                'clear_browsing_data' in e['event_value']
            ):
                return ['feature: settings_clear_browsing_data']
            return False

        @staticmethod
        def settings_change_locale_feature(e: Series):
            if (
                e['event_value'] == 'pref_locale'
            ):
                return ['feature: settings_change_locale']
            return False

        @staticmethod
        def settings_change_collection_telemetry_feature(e: Series):
            if (
                e['event_object'] == 'setting' and
                e['event_value'] == 'telemetry'
            ):
                return ['feature: settings_change_collection_telemetry']
            return False

        @staticmethod
        def visit_settings_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'menu' and
                e['event_value'] == 'settings'
            ):
                return ['feature: visit_settings']
            return False

        @staticmethod
        def visit_download_feature(e: Series):
            if (
                e['event_value']  == 'download' or
                (
                    e['event_method'] == 'open' and
                    e['event_object'] == 'panel' and
                    e['event_value'] == 'file'
            )):
                return ['feature: visit_download']
            return False

        @staticmethod
        def clean_download_file_feature(e: Series):
            if (
                e['event_method']  in ['remove','delete'] and
                e['event_object'] == 'panel' and
                e['event_value'] == 'file'
            ):
                return ['feature: clean_download_file']
            return False

        @staticmethod
        def exit_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'menu' and
                e['event_value'] == 'exit'
            ):
                return ['feature: exit']
            return False

        @staticmethod
        def give_feedback_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                (
                    e['event_object'] == 'feedback' or
                    (
                        e['event_value'] is not None and 'feedback' in e['event_value']
                    )
                )
            ):
                return ['feature: give_feedback']
            return False

        @staticmethod
        def find_in_page_feature(e: Series):
            if (
                e['event_object'] == 'find_in_page' or
                e['event_value'] == 'find_in_page'
            ):
                return ['feature: find_in_page']
            return False

        @staticmethod
        def forward_page_feature(e: Series):
            if (
                e['event_value'] == 'forward'
            ):
                return ['feature: forward_page']
            return False

        @staticmethod
        def fullscreen_feature(e: Series):
            if (
                e['event_method'] == 'fullscreen'
            ):
                return ['feature: fullscreen']
            return False

        @staticmethod
        def landscape_mode_feature(e: Series):
            if (
                e['event_object'] == 'landscape_mode'
            ):
                return ['feature: landscape_mode']
            return False

        @staticmethod
        def visit_topsite_feature(e: Series):
            if (
                e['event_method'] == 'open' and
                e['event_object'] == 'home' and
                e['event_value'] == 'link'
            ):
                return ['feature: visit_topsite']
            return False

        @staticmethod
        def visit_topsite_bukalapak_feature(e: Series):
            if (
                e['event_method'] == 'open' and
                e['event_object'] == 'home' and
                e['event_value'] == 'link' and
                e['extra_key'] == 'source' and
                e['extra_value'].lower() in partner_list
            ):
                return ['tags: visit_topsite_bukalapak',
                        'source: %s' % (e['extra_value'].lower()),
                        'partner: True']
            return False

        @staticmethod
        def remove_topsite_feature(e: Series):
            if (
                e['event_method'] == 'remove' and
                e['event_object'] == 'home' and
                e['event_value'] == 'link'
            ):
                return ['feature: remove_topsite']
            return False

        @staticmethod
        def change_night_mode_feature(e: Series):
            if (
                e['event_method'] == 'change' and
                e['event_value'] is not None and
                'night_mode' in e['event_value']
            ):
                return ['feature: change_night_mode']
            return False

        @staticmethod
        def pin_shortcut_feature(e: Series):
            if (
                e['event_method'] == 'pin_shortcut'
            ):
                return ['feature: pin_shortcut']
            return False

        @staticmethod
        def private_mode_feature(e: Series):
            if ((
                e['event_method'] != 'show' and
                (
                     e['event_object'] is not None and
                     'private_' in e['event_object']
                )) or (

                e['event_value'] not in ['show','launch'] and
                (
                     e['event_value'] is not None and
                     'private_' in e['event_value']
            ))):
                return ['feature: private_mode']
            return False

        @staticmethod
        def reload_page_feature(e: Series):
            if (
                e['event_value'] == 'reload_page'
            ):
                return ['feature: reload_page']
            return False

        @staticmethod
        def screenshot_feature(e: Series):
            if (
                e['event_method'] != 'share' and
                (
                    e['event_value'] == 'capture' or
                    e['event_object'] == 'capture'
            )):
                return ['feature: screenshot']
            return False

        @staticmethod
        def tab_swipe_feature(e: Series):
            if (
                # enter & leave
                (
                    e['event_method'] in ['click','start','end','clear'] and
                    ((
                        e['event_value'] is not None and
                        'tab_swipe' in e['event_value']
                    ) or
                        e['event_object'] == 'tab_swipe'
                    ) and

                    e['extra_key'] == 'vertical' and
                    e['extra_value'] == 'shopping'
                ) or

                # visit tab swipe setting
                (
                    e['event_method'] == 'change' and
                    e['event_object'] == 'setting' and
                    e['event_value'] == 'tab_swipe'
            )):
                return ['feature: tab_swipe']
            return False

        @staticmethod
        def tab_swipe_feed_feature(e: Series):
            if (
                e['event_method'] == 'end' and
                e['event_object'] == 'tab_swipe' and
                e['extra_key'] == 'feed'
            ):
                return ['feed: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def tab_swipe_source_feature(e: Series):
            if (
                e['event_method'] == 'end' and
                e['event_object'] == 'tab_swipe' and
                e['extra_key'] == 'source'
            ):
                return ['source: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def tab_swipe_partner_feature(e: Series):
            if (
                e['event_method'] == 'end' and
                e['event_object'] == 'tab_swipe' and
                e['extra_key'] == 'feed' and
                e['extra_value'].lower() in partner_list
            ):
                return ['partner: True']
            return False

        @staticmethod
        def browse_feature(e: Series):
            if (
                e['event_object'] == 'browser_contextmenu' or
                (
                    e['event_method'] == 'long_press' and
                    e['event_object'] == 'browser'
            )):
                return ['feature: browse']
            return False

        @staticmethod
        def pre_search_feature(e: Series):
            if ((
                e['event_method'] in ['show','cancel','clear'] and
                e['event_object'] == 'search_bar'
            ) or (
                e['event_method'] == 'long_press' and
                e['event_object'] == 'search_suggestion'
            )):
                return ['feature: pre_search']
            return False

        @staticmethod
        def search_feature(e: Series):
            if ((
                e['event_method'] in ['type_query','select_query'] and
                e['event_object'] == 'search_bar'
            ) or (
                e['event_method'] == 'click' and
                e['event_object'] == 'quicksearch'
            ) or (
                e['event_method'] == 'open' and
                e['event_object'] == 'search_bar' and
                e['event_value'] == 'link'
            )):
                return ['feature: search']
            return False

        @staticmethod
        def search_keyword_google_feature(e: Series):
            if (
                e['event_method'] in ['type_query','select_query'] and
                e['event_object'] == 'search_bar' and
                e['settings_key'] == 'pref_search_engine' and
                (
                    e['settings_value'].lower() == 'google' or
                    e['settings_value'] is None
            )):
                return ['tags: keyword_search',
                        'source: google',
                        'partner: True']
            return False

        @staticmethod
        def search_keyword_feature(e: Series):
            if (
                e['event_method'] in ['type_query','select_query'] and
                e['event_object'] == 'search_bar'
            ):
                return ['tags: keyword_search']
            return False

        @staticmethod
        def search_quicksearch_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'quicksearch'
            ):
                return ['tags: quicksearch']
            return False

        @staticmethod
        def search_quicksearch_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'quicksearch' and
                e['extra_key'] == 'engine' and
                e['extra_value'].lower() in partner_list
            ):
                return ['tags: quicksearch',
                        'source: %s' % (e['extra_value'].lower()),
                        'partner: True']
            return False

        @staticmethod
        def search_url_feature(e: Series):
            if (
                e['event_method'] == 'open' and
                e['event_object'] == 'search_bar' and
                e['event_value'] == 'link'
            ):
                return ['tags: url_search']
            return False

        @staticmethod
        def settings_change_search_engine_feature(e: Series):
            if (
                e['event_method'] in ['change','click'] and
                e['event_object'] == 'setting' and
                e['event_value'] == 'search_engine'
            ):
                return ['feature: settings_change_search_engine']
            return False

        @staticmethod
        def share_feature(e: Series):
            if (
                e['event_method'] == 'share' or
                (
                    e['event_object'] == 'setting' and
                    e['event_value'] is not None and
                    'share_with_friends' in e['event_value']
            )):
                return ['feature: share']
            return False

        @staticmethod
        def themetoy_feature(e: Series):
            if (
                e['event_object'] == 'themetoy'
            ):
                return ['feature: themetoy']
            return False

        @staticmethod
        def change_turbo_mode_feature(e: Series):
            if (
                e['event_method'] == 'change' and
                e['event_value'] is not None and
                'turbo' in e['event_value']
            ):
                return ['feature: change_turbo_mode']
            return False

        @staticmethod
        def vpn_feature(e: Series):
            if ((
                e['event_method'] == 'click' and
                e['event_object'] is not None and
                'vpn' in e['event_object']
            ) or (
                e['event_method'] == 'click' and
                e['event_value'] is not None and
                'vpn' in e['event_value']
            )):
                return ['feature: vpn']
            return False

        @staticmethod
        def settings_learn_more_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'setting' and
                e['event_value'] == 'learn_more'
            ):
                return ['feature: settings_learn_more']
            return False

        @staticmethod
        def launch_app_feature(e: Series):
            if (
                e['event_method'] == 'launch' and
                e['event_object'] == 'app'
            ):
                return ['feature: launch_app']
            return False

        @staticmethod
        def launch_app_from_external_feature(e: Series):
            if (
                e['event_method'] == 'launch' and
                e['event_object'] == 'app' and
                e['event_value'] == 'external_app'
            ):
                return ['tags: launch_app_from_external']
            return False

        @staticmethod
        def launch_app_from_launcher_feature(e: Series):
            if (
                e['event_method'] == 'launch' and
                e['event_object'] == 'app' and
                e['event_value'] == 'launcher'
            ):
                return ['tags: launch_app_from_launcher']
            return False

        @staticmethod
        def launch_app_from_shortcut_feature(e: Series):
            if (
                e['event_method'] == 'launch' and
                e['event_object'] == 'app' and
                e['event_value'] in ['shortcut','private_mode']
            ):
                return ['tags: launch_app_from_shortcut']
            return False

    class ShoppingVertical:
        """Shopping Vertical."""

        @staticmethod
        def lifefeed_e_ticket_feature(e: Series):
            if (
                e['event_value'] == 'lifefeed_ec'
            ):
                return ['feature: lifefeed',
                        'category: e_ticket']
            return False

        @staticmethod
        def lifefeed_e_ticket_click_tags_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_ec' and
                e['extra_key'] == 'category'
            ):
                return ['component_type_id: 9', #icon_card
                        'tags: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_e_ticket_click_source_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_ec' and
                e['extra_key'] == 'source'
            ):
                return ['component_type_id: 9', #icon_card
                        'feed: %s' % (e['extra_value'].lower()),
                        'source: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_e_ticket_click_partner_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_ec' and
                e['extra_key'] == 'source' and
                e['extra_value'].lower() in partner_list
            ):
                return ['partner: True']
            return False

        @staticmethod
        def lifefeed_coupon_feature(e: Series):
            if (
                e['event_value'] == 'lifefeed_promo'
            ):
                return ['feature: lifefeed',
                       'category: coupon']
            return False

        @staticmethod
        def lifefeed_coupon_click_list_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_promo' and
                e['extra_key'] == 'feed' and
                e['extra_value'] == 'list'
            ):
                return ['component_type_id: 7'] #list
            return False

        @staticmethod
        def lifefeed_coupon_click_banner_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_promo' and
                e['extra_key'] == 'feed' and
                e['extra_value'] == 'banner'
            ):
                return ['component_type_id: 6'] #banner
            return False

        @staticmethod
        def lifefeed_coupon_click_source_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_promo' and
                e['extra_key'] == 'source'
            ):
                return ['feed: %s' % (e['extra_value'].lower()),
                        'source: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_coupon_click_tags_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_promo' and
                e['extra_key'] == 'subcategory'
            ):
                return ['tags: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_coupon_click_partner_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_value'] == 'lifefeed_promo' and
                e['extra_key'] == 'source' and
                e['extra_value'].lower() in partner_list
            ):
                return ['partner: True']
            return False

    class LifestyleVertical:
        """Lifestyle Vertical."""
        @staticmethod
        def lifefeed_news_feature(e: Series):
            if (
                e['event_value'] == 'lifefeed_news'
            ):
                return ['feature: lifefeed_news']
            return False

        @staticmethod
        def lifefeed_news_category_feature(e: Series):
            if (
                e['event_method'] == 'open' and
                e['event_value'] == 'lifefeed_news' and
                e['extra_key'] == 'category'
            ):
                return ['category: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_news_click_feed_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'panel' and
                e['event_value'] == 'lifefeed_news' and
                e['extra_key'] == 'feed'
            ):
                return ['component_type_id: 7', #list
                        'feed: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_news_click_source_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'panel' and
                e['event_value'] == 'lifefeed_news' and
                e['extra_key'] == 'source'
            ):
                return ['component_type_id: 7', #list
                        'source: %s' % (e['extra_value'].lower())]
            return False

        @staticmethod
        def lifefeed_news_click_partner_feature(e: Series):
            if (
                e['event_method'] == 'click' and
                e['event_object'] == 'panel' and
                e['event_value'] == 'lifefeed_news' and
                e['extra_key'] == 'feed' and
                e['extra_value'].lower() in partner_list
            ):
                return ['partner: True']
            return False
