from google.cloud import bigquery

BQ_PROJECT = {"dataset": "test", "location": "US"}

SELECT_TABLE = {
    "id": BQ_PROJECT,
    "type": "table",
    "params": {"src": "bigquery-public-data.usa_names.usa_1910_2013", "dest": "new"},
    "query": "select_table",
    "append": True,
}
