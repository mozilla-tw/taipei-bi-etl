from google.cloud import bigquery

BQ_PROJECT = {"dataset": "test", "location": "US"}

SELECT_TABLE = {
    "type": "table",
    "params": {
        "src": "bigquery-public-data.usa_names.usa_1910_2013",
        "dest": "new",
        **BQ_PROJECT,
    },
    "query": "select_table",
    "append": True,
}
