"""Configurations for data source, data schema, and data destinations."""
# flake8: noqa
import numpy as np
import os


SOURCES = {
    "bukalapak": {
        "type": "api",
        "url": "https://bukalapak.api.hasoffers.com/Apiv3/json?api_key={api_key}&Target=Affiliate_Report&Method=getConversions&fields[]=Stat.session_ip&fields[]=Stat.ip&fields[]=Stat.id&fields[]=ConversionsMobile.windows_aid_sha1&fields[]=ConversionsMobile.windows_aid_md5&fields[]=ConversionsMobile.windows_aid&fields[]=ConversionsMobile.unknown_id&fields[]=ConversionsMobile.mobile_carrier&fields[]=ConversionsMobile.ios_ifa_sha1&fields[]=ConversionsMobile.ios_ifa_md5&fields[]=ConversionsMobile.ios_ifa&fields[]=ConversionsMobile.google_aid_sha1&fields[]=ConversionsMobile.google_aid_md5&fields[]=ConversionsMobile.google_aid&fields[]=Stat.year&fields[]=Stat.week&fields[]=Stat.user_agent&fields[]=Stat.source&fields[]=Stat.session_datetime&fields[]=Stat.sale_amount&fields[]=Stat.refer&fields[]=Stat.pixel_refer&fields[]=Stat.offer_url_id&fields[]=Stat.offer_id&fields[]=Stat.month&fields[]=Stat.is_adjustment&fields[]=Stat.hour&fields[]=Stat.goal_id&fields[]=Stat.datetime&fields[]=Stat.date&fields[]=Stat.currency&fields[]=Stat.count_approved&fields[]=Stat.conversion_status&fields[]=Stat.approved_payout&fields[]=Stat.affiliate_info5&fields[]=Stat.affiliate_info4&fields[]=Stat.affiliate_info3&fields[]=Stat.affiliate_info2&fields[]=Stat.affiliate_info1&fields[]=Stat.ad_id&fields[]=PayoutGroup.name&fields[]=PayoutGroup.id&fields[]=OfferUrl.preview_url&fields[]=OfferUrl.name&fields[]=OfferUrl.id&fields[]=Offer.name&fields[]=Goal.name&fields[]=Country.name&fields[]=ConversionsMobile.device_os&fields[]=ConversionsMobile.device_os_version&fields[]=ConversionsMobile.device_model&fields[]=ConversionsMobile.device_brand&fields[]=ConversionsMobile.affiliate_unique5&fields[]=ConversionsMobile.affiliate_unique4&fields[]=ConversionsMobile.affiliate_unique3&fields[]=ConversionsMobile.affiliate_unique2&fields[]=ConversionsMobile.affiliate_unique1&fields[]=ConversionsMobile.affiliate_click_id&fields[]=ConversionMeta.note&fields[]=Browser.id&fields[]=Browser.display_name&filters[Stat.date][conditional]=BETWEEN&filters[Stat.date][values][]={start_date}&filters[Stat.date][values][]={end_date}&sort[Stat.datetime]=desc&limit={limit}&page={page}",
        "api_key": os.environ.get('BUKALAPAK_API_KEY'),
        "load": True,
        "request_interval": 1,
        "cache_file": True,
        "force_load_cache": False,
        "date_format": "%Y-%m-%d",
        "file_format": "json",
        "json_path": "response.data.data",
        "json_path_page_count": "response.data.pageCount",
        "page_size": 100,
        "country_code": "ID",   # for detecting timezone,
        "date_fields": ["Stat.date", "Stat.datetime", "Stat.session_datetime"],
    },
}
SCHEMA = [
    ("source", np.dtype(object).type),
    ("country", np.dtype(object).type),
    ("os", np.dtype(object).type),
    ("utc_datetime", np.datetime64),
    ("tz", np.dtype(object).type),
    ("currency", np.dtype(object).type),
    ("sales_amount", np.dtype(float).type),
    ("payout", np.dtype(float).type),
    ("fx_defined1", np.dtype(object).type),
    ("fx_defined2", np.dtype(object).type),
    ("fx_defined3", np.dtype(object).type),
    ("fx_defined4", np.dtype(object).type),
    ("fx_defined5", np.dtype(object).type),
    ("conversion_status", np.dtype(object).type),
]
DESTINATIONS = {
    "gcs": {
        "bucket": "moz-fx-data-derived-datasets-analysis",
        "prefix": "taipei-bi/",
    },
    "fs": {
        "prefix": "./data/",
        "file_format": "jsonl",
        "date_field": "utc_datetime",
    }
}
