# em-partners-etl
Docker image for partner data ETL.

## Getting Started
1. [Install docker](https://docs.docker.com/install/)
2. `cp settings.py.sample settings.py` to customize your settings
3. Build docker image `docker build --rm -t em-partner-etl-job .` don't forget the ending dot.
4. Run docker instance from the image `docker run em-partner-etl-job`

It will run in tempfs (in-memory storage), and won't left anything on your harddrive.

* If you need to check/persist the intermediate output on your file system, 
<br>you can run with [bind mount](https://docs.docker.com/storage/bind-mounts/) `docker run -v {path on your filesystem}:/app/data em-partner-etl-job`
* If you only need a persist inetermediate output, use [volumes](https://docs.docker.com/storage/volumes/)
* For other available ETL options (e.g. specify date rage), run `docker run em-partner-etl-job --help`
