# taipei-bi-etl
Docker image for taipei-bi team ETL.

## Getting Started
1. [Install docker](https://docs.docker.com/install/)
2. `cp settings.py.sample settings.py` to customize your settings
3. Build docker image `docker build --rm -t taipei-bi-etl-img .` don't forget the ending dot.
4. Run docker instance from the image `docker run taipei-bi-etl-img`

It will run in tempfs (in-memory storage), and won't left anything on your harddrive.

* If you need to check/persist the intermediate output on your file system, 
<br>you can run with [bind mount](https://docs.docker.com/storage/bind-mounts/).
<br>`docker run -v {path on your filesystem}:/app/data taipei-bi-etl-img`
* To be able to authenticate gcloud when testing locally, 
<br>you may want to mount your local gcloud config:
<br>`docker run -v {path of your ~/.config folder}:/root/.config -v {path on your filesystem}:/app/data taipei-bi-etl-img`
* If you only need a persist inetermediate output, use [volumes](https://docs.docker.com/storage/volumes/).
* For other available ETL options (e.g. specify date rage), run `docker run taipei-bi-etl-img --help`
