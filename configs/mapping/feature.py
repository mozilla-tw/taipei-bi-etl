"""Filter events and label with features."""
from pandas import Series


class Feature:
    
    global partner_list
    partner_list = ['bukalapak','flipkart','jd.id','gamezop']
    
    
    """
    ##########################
    Browser Feature
    ##########################
    """
    
    @staticmethod
    def add_tab(e: Series):
        if (e['event_method'] == 'add' and
           e['event_object'] == 'tab'): 
            return ['feature: add_tab']
        return False
    
    @staticmethod
    def change_tab(e: Series):
        if (
            e['event_method'] == 'change' and
            e['event_object'] == 'tab'
       ): 
            return ['feature: change_tab']
        return False
    
    @staticmethod
    def close_all_tab(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'close_all' and
            e['event_value'] == 'tab_tray'
       ): 
            return ['feature: close_all_tab']
        return False
    
    @staticmethod
    def remove_tab(e: Series):
        if (
            e['event_method'] in ['remove', 'swipe'] and
            e['event_object'] == 'tab' and
            e['event_value'] in ['tab_tray','tab_swipe']
       ): 
            return ['feature: remove_tab']
        return False
    
    @staticmethod
    def remove_tab(e: Series):
        if (
            e['event_method'] in ['remove', 'swipe'] and
            e['event_object'] == 'tab' and
            e['event_value'] == 'tab_tray'
        ): 
            return ['feature: remove_tab']
        return False
    
    @staticmethod
    def change_block_image(e: Series):
        if (
            e['event_value'] == 'block_image'
        ): 
            return ['feature: change_block_image']
        return False
    
    @staticmethod
    def bookmark(e: Series):
        if (
            e['event_method'] != 'share' and 
            e['event_value'] == 'bookmark' 
        ):
            return ['feature: bookmark']
        return False
    
    @staticmethod
    def visit_history(e: Series):
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
    def clean_history(e: Series):
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
    def clear_cache(e: Series):
        if (
            e['event_value'] == 'clear_cache'
        ): 
            return ['feature: clear_cache']
        return False
    
    @staticmethod
    def change_default_browser(e: Series):
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
    def settings_change_download_location(e: Series):
        if (
            e['event_method'] in ['click','change'] and
            e['event_value'] is not None and
            'save_downloads_to' in e['event_value']
        ): 
            return ['feature: settings_change_download_location']
        return False
    
    @staticmethod
    def settings_clear_browsing_data(e: Series):
        if (
            e['event_value'] is not None and
            'clear_browsing_data' in e['event_value']
        ): 
            return ['feature: settings_clear_browsing_data']
        return False
    
    @staticmethod
    def settings_change_locale(e: Series):
        if (
            e['event_value'] == 'pref_locale'
        ): 
            return ['feature: settings_change_locale']
        return False
    
    @staticmethod
    def settings_change_collection_telemetry(e: Series):
        if (
            e['event_object'] == 'setting' and
            e['event_value'] == 'telemetry'
        ): 
            return ['feature: settings_change_collection_telemetry']
        return False
    
    @staticmethod
    def visit_settings(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'menu' and
            e['event_value'] == 'settings'
        ): 
            return ['feature: visit_settings']
        return False
    
    @staticmethod
    def visit_download(e: Series):
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
    def clean_download_file(e: Series):
        if (
            e['event_method']  in ['remove','delete'] and
            e['event_object'] == 'panel' and 
            e['event_value'] == 'file'
        ): 
            return ['feature: clean_download_file']
        return False
    
    @staticmethod
    def exit(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'menu' and 
            e['event_value'] == 'exit'
        ): 
            return ['feature: exit']
        return False
    
    @staticmethod
    def give_feedback(e: Series):
        if (
            e['event_method'] == 'click' and
            (
                e['event_object'] == 'feedback' or
                (
                    e['event_value'] is not None and
                    'feedback' in e['event_value']
        ))): 
            return ['feature: give_feedback']
        return False
    
    @staticmethod
    def find_in_page(e: Series):
        if (
            e['event_object'] == 'find_in_page' or
            e['event_value'] == 'find_in_page'
        ): 
            return ['feature: find_in_page']
        return False
    
    @staticmethod
    def forward_page(e: Series):
        if (
            e['event_value'] == 'forward'
        ): 
            return ['feature: forward_page']
        return False
    
    @staticmethod
    def fullscreen(e: Series):
        if (
            e['event_method'] == 'fullscreen'
        ): 
            return ['feature: fullscreen']
        return False
    
    @staticmethod
    def landscape_mode(e: Series):
        if (
            e['event_object'] == 'landscape_mode'
        ): 
            return ['feature: landscape_mode']
        return False
    
    @staticmethod
    def visit_topsite(e: Series):
        if (
            e['event_method'] == 'open' and 
            e['event_object'] == 'home' and 
            e['event_value'] == 'link'
        ): 
            return ['feature: visit_topsite']
        return False
    
    @staticmethod
    def visit_topsite_bukalapak(e: Series):
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
    def remove_topsite(e: Series):
        if (
            e['event_method'] == 'remove' and 
            e['event_object'] == 'home' and 
            e['event_value'] == 'link'
        ): 
            return ['feature: remove_topsite']
        return False
    
    @staticmethod
    def change_night_mode(e: Series):
        if (
            e['event_method'] == 'change' and
            e['event_value'] is not None and
            'night_mode' in e['event_value']
        ): 
            return ['feature: change_night_mode']
        return False
    
    @staticmethod
    def pin_shortcut(e: Series):
        if (
            e['event_method'] == 'pin_shortcut'
        ): 
            return ['feature: pin_shortcut']
        return False
    
    @staticmethod
    def private_mode(e: Series):
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
    def reload_page(e: Series):
        if (
            e['event_value'] == 'reload_page'
        ): 
            return ['feature: reload_page']
        return False
    
    @staticmethod
    def screenshot(e: Series):
        if (
            e['event_method'] != 'share' and 
            (
                e['event_value'] == 'capture' or
                e['event_object'] == 'capture'
        )): 
            return ['feature: screenshot']
        return False
    
    @staticmethod
    def tab_swipe(e: Series):
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
    def tab_swipe_feed(e: Series):
        if (
            e['event_method'] == 'end' and
            e['event_object'] == 'tab_swipe' and
            e['extra_key'] == 'feed'
        ): 
            return ['feed: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def tab_swipe_source(e: Series):
        if (
            e['event_method'] == 'end' and
            e['event_object'] == 'tab_swipe' and
            e['extra_key'] == 'source'
        ): 
            return ['source: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def tab_swipe_partner(e: Series):
        if (
            e['event_method'] == 'end' and
            e['event_object'] == 'tab_swipe' and
            e['extra_key'] == 'feed' and
            e['extra_value'].lower() in partner_list
        ): 
            return ['partner: True']
        return False
    
    @staticmethod
    def browse(e: Series):
        if (
            e['event_object'] == 'browser_contextmenu' or 
            (
                e['event_method'] == 'long_press' and
                e['event_object'] == 'browser'
        )): 
            return ['feature: browse']
        return False
    
    @staticmethod
    def pre_search(e: Series):
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
    def search(e: Series):
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
    def search_keyword_google(e: Series):
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
    def search_keyword(e: Series):
        if (
            e['event_method'] in ['type_query','select_query'] and
            e['event_object'] == 'search_bar'
        ): 
            return ['tags: keyword_search']
        return False
    
    @staticmethod
    def search_quicksearch(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'quicksearch'
        ): 
            return ['tags: quicksearch']
        return False
    
    @staticmethod
    def search_quicksearch(e: Series):
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
    def search_url(e: Series):
        if (
            e['event_method'] == 'open' and
            e['event_object'] == 'search_bar' and
            e['event_value'] == 'link'
        ): 
            return ['tags: url_search']
        return False
    
    @staticmethod
    def settings_change_search_engine(e: Series):
        if (
            e['event_method'] in ['change','click'] and
            e['event_object'] == 'setting' and
            e['event_value'] == 'search_engine'
        ): 
            return ['feature: settings_change_search_engine']
        return False
    
    @staticmethod
    def share(e: Series):
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
    def themetoy(e: Series):
        if (
            e['event_object'] == 'themetoy'
        ): 
            return ['feature: themetoy']
        return False
            
    @staticmethod
    def change_turbo_mode(e: Series):
        if (
            e['event_method'] == 'change' and
            e['event_value'] is not None and 
            'turbo' in e['event_value']
        ): 
            return ['feature: change_turbo_mode']
        return False
            
    @staticmethod
    def vpn(e: Series):
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
    def settings_learn_more(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'setting' and
            e['event_value'] == 'learn_more'
        ): 
            return ['feature: settings_learn_more']
        return False 
    
    @staticmethod
    def launch_app(e: Series):
        if (
            e['event_method'] == 'launch' and
            e['event_object'] == 'app'
        ): 
            return ['feature: launch_app']
        return False 
    
    @staticmethod
    def launch_app_from_external(e: Series):
        if (
            e['event_method'] == 'launch' and
            e['event_object'] == 'app' and 
            e['event_value'] == 'external_app' 
        ): 
            return ['tags: launch_app_from_external']
        return False 
    
    @staticmethod
    def launch_app_from_launcher(e: Series):
        if (
            e['event_method'] == 'launch' and
            e['event_object'] == 'app' and 
            e['event_value'] == 'launcher' 
        ): 
            return ['tags: launch_app_from_launcher']
        return False 
    
    @staticmethod
    def launch_app_from_shortcut(e: Series):
        if (
            e['event_method'] == 'launch' and
            e['event_object'] == 'app' and 
            e['event_value'] in ['shortcut','private_mode']
        ): 
            return ['tags: launch_app_from_shortcut']
        return False 
    
    
    
    
    
    
    """
    ##########################
    Vertical Feature - Shopping
    ##########################
    """
    
    @staticmethod
    def lifefeed_e_ticket(e: Series):
        if (
            e['event_value'] == 'lifefeed_ec'
        ): 
            return ['feature: lifefeed',
                    'category: e_ticket']
        return False
    
    @staticmethod
    def lifefeed_e_ticket_click_tags(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_ec' and
            e['extra_key'] == 'category'
        ): 
            return ['component_type_id: 9', #icon_card 
                    'tags: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def lifefeed_e_ticket_click_source(e: Series):
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
    def lifefeed_e_ticket_click_partner(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_ec' and
            e['extra_key'] == 'source' and 
            e['extra_value'].lower() in partner_list
        ): 
            return ['partner: True']
        return False
    
    @staticmethod
    def lifefeed_coupon(e: Series):
        if (
            e['event_value'] == 'lifefeed_promo'
        ): 
            return ['feature: lifefeed',
                   'category: coupon']
        return False
    
    @staticmethod
    def lifefeed_coupon_click_list(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_promo' and
            e['extra_key'] == 'feed' and
            e['extra_value'] == 'list'
        ): 
            return ['component_type_id: 7'] #list
        return False
    
    @staticmethod
    def lifefeed_coupon_click_banner(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_promo' and
            e['extra_key'] == 'feed' and
            e['extra_value'] == 'banner'
        ): 
            return ['component_type_id: 6'] #banner
        return False
    
    @staticmethod
    def lifefeed_coupon_click_source(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_promo' and
            e['extra_key'] == 'source'
        ): 
            return ['feed: %s' % (e['extra_value'].lower()),
                    'source: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def lifefeed_coupon_click_tags(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_promo' and
            e['extra_key'] == 'subcategory'
        ): 
            return ['tags: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def lifefeed_coupon_click_partner(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_value'] == 'lifefeed_promo' and
            e['extra_key'] == 'source' and
            e['extra_value'].lower() in partner_list
        ): 
            return ['partner: True']
        return False
    
    
    
    
    """
    ##########################
    Vertical Feature - Lifestyle
    ##########################
    """
    
    @staticmethod
    def lifefeed_news(e: Series):
        if (
            e['event_value'] == 'lifefeed_news'
        ): 
            return ['feature: lifefeed_news']
        return False
    
    @staticmethod
    def lifefeed_news_category(e: Series):
        if (
            e['event_method'] == 'open' and
            e['event_value'] == 'lifefeed_news' and
            e['extra_key'] == 'category'
        ): 
            return ['category: %s' % (e['extra_value'].lower())]
        return False
    
    @staticmethod
    def lifefeed_news_click_feed(e: Series):
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
    def lifefeed_news_click_source(e: Series):
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
    def lifefeed_news_click_partner(e: Series):
        if (
            e['event_method'] == 'click' and
            e['event_object'] == 'panel' and
            e['event_value'] == 'lifefeed_news' and
            e['extra_key'] == 'feed' and 
            e['extra_value'].lower() in partner_list
        ): 
            return ['partner: True']
        return False
    


    
    