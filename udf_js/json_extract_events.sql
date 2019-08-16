CREATE TEMP FUNCTION
  udf_js_json_extract_events (input STRING)

  RETURNS ARRAY<STRUCT<
  event_timestamp INT64,
  event_category STRING,
  event_object STRING,
  event_method STRING,
  event_value STRING,
  event_extra ARRAY<STRUCT<key STRING, value STRING>>
  >>
  LANGUAGE js AS """
    if (input == null) {
      return null;
    }
    var parsed = JSON.parse(input);
    var result = [];
    parsed.forEach(event => {
        var structured = {
          "event_timestamp": event[0],
          "event_category": event[1],
          "event_method": event[2],
          "event_object": event[3],
          "event_value": event[4],
          "event_extra": []
        }
        for (var key in event[5]) {
          structured.event_extra.push({"key": key, "value": event[5][key]})
        }
        result.push(structured)
    });
    return result;
""";