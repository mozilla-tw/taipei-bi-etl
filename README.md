# taipei-bi-etl
Docker image for taipei-bi team ETL tasks.

## Getting Started
1. [Install docker](https://docs.docker.com/install/)
2. `cp settings.py.sample settings.py` to customize your settings
3. Build docker image `docker build -t taipei-bi-etl-img .` don't forget the ending dot.
4. Run docker instance from the image `docker run taipei-bi-etl-img`

It will run in tempfs (in-memory storage), and won't left anything on your harddrive.

* If you need to check/persist the intermediate output on your file system, 
<br>you can run with [bind mount](https://docs.docker.com/storage/bind-mounts/).
<br>`docker run -v {path on your filesystem}:/app/data taipei-bi-etl-img`
* If you only need a persist inetermediate output, use [volumes](https://docs.docker.com/storage/volumes/).
* To be able to authenticate gcloud when testing locally, 
<br>you may want to mount your local gcloud config:
<br>`docker run -v {path of your ~/.config folder}:/root/.config -v {path on your filesystem}:/app/data taipei-bi-etl-img`
* Note that the default timezone of the Docker container is UTC,
<br> use `-e` run option to adjust the container timezone: 
<br>`docker run -e="Asia/Taipei" taipei-bi-etl-img` 
* For other available ETL options (e.g. specify date rage), run `docker run taipei-bi-etl-img --help`
* A full example of the docker command would be like:<br>
`docker build -t taipei-bi-etl-img . && docker run -v /Users/eddielin/taipei-bi-etl/data:/app/data -v /Users/eddielin/.config:/root/.config --name taipei-bi-etl taipei-bi-etl-img --task revenue --step e --source google_search`
* For more options of the etl tasks, run:<br>
`docker build -t taipei-bi-etl-img . && docker run -v /Users/eddielin/taipei-bi-etl/data:/app/data -v /Users/eddielin/.config:/root/.config --name taipei-bi-etl taipei-bi-etl-img --help`

## Technical References
- [pandas](https://pandas.pydata.org/pandas-docs/stable/getting_started/10min.html)
- Docker
    - [Getting started](https://docs.docker.com/get-started/)
    - [Storage overview](https://docs.docker.com/storage/)
    - [mozilla's Docker example with GCP](https://github.com/mozilla/bigquery-etl/blob/master/script/generate_sql)
    - [mozilla's example running on GKE using Dockerflow](https://github.com/mozilla/mozilla-schema-generator)
    - [mozilla's Dockerflow](https://github.com/mozilla-services/Dockerflow)
- BigQuery
    - [One page functions and operators](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators)
    - [Client libraries](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
    - [External data source (query w/o loading)](https://cloud.google.com/bigquery/external-data-sources)
    - [Load data (from GCS)](https://cloud.google.com/bigquery/docs/loading-data)
- [Cloud Storage](https://cloud.google.com/storage/docs/how-to)
