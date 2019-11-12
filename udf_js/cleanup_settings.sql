CREATE OR REPLACE FUNCTION `%s.%s`.udf_js_cleanup_settings (
  build_id STRING, settings ARRAY<STRUCT<key STRING,value STRING>>
)
RETURNS ARRAY<STRUCT<key STRING,value STRING>>
  LANGUAGE js
AS """
    if (settings == null) {
      return null;
    }
    var result = [];
    for (var i in settings) {
      var k = settings[i]["key"].toLowerCase();
      var v = (null == settings[i]["value"] ? "" : settings[i]["value"].toLowerCase());
      result.push({"key": k, "value": v});
    }
    return result;
""";