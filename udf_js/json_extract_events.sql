CREATE OR REPLACE FUNCTION `%s.%s`.udf_js_json_extract_events (
  input STRING
)
RETURNS ARRAY<STRUCT< event_timestamp INT64,event_category STRING,event_object STRING,event_method STRING,event_value STRING,event_extra ARRAY<STRUCT<key STRING,value STRING>> >>
  LANGUAGE js
AS """
    if (input == null) {
      return null;
    }
    var parsed = JSON.parse(input);
    var result = [];
    parsed.forEach(event => {
        var structured = {
          "event_timestamp": null == event[0] ? 0 : event[0],
          "event_category": null == event[1] ? "" : event[1].toLowerCase(),
          "event_method": null == event[2] ? "" : event[2].toLowerCase(),
          "event_object": null == event[3] ? "" : event[3].toLowerCase(),
          "event_value": null == event[4] ? "" : event[4].toLowerCase(),
          "event_extra": []
        }
        for (var key in event[5]) {
          structured.event_extra.push({"key": key.toLowerCase(), "value": (null == event[5][key] ? "" : event[5][key].toLowerCase())})
        }
        result.push(structured)
    });
    return result;
""";