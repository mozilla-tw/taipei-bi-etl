CREATE OR REPLACE FUNCTION `%s.%s`.udf_js_cleanup_extra (
  build_id STRING, event_extra ARRAY<STRUCT<key STRING,value STRING>>
)
RETURNS ARRAY<STRUCT<key STRING,value STRING>>
  LANGUAGE js
AS """
    if (event_extra == null) {
      return null;
    }
    var result = [];
    for (var i in event_extra) {
      var k = event_extra[i]["key"].toLowerCase();
      var v = (null == event_extra[i]["value"] ? "" : event_extra[i]["value"].toLowerCase());
      if (k == "session_time" && parseInt(v) < 0) {
        v = "0";
      }
      else if (k == "url_counts") {
        // Handle url_count issue: https://github.com/mozilla-tw/mango/issues/818
        // Need to stop patching data on latest builds once fixed
        v = (parseInt(v) + 1).toString();
      }
      result.push({"key": k, "value": v});
    }
    return result;
""";