ARG PYTHON_VERSION=3.7

FROM python:3-slim
COPY requirements ./requirements
RUN pip install --no-cache-dir -r requirements/requirements.txt
RUN pip install --no-cache-dir -r requirements/test_requirements.txt
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev && \
    pip install -r requirements/requirements.txt && \
    pip install -r requirements/test_requirements.txt

FROM python:${PYTHON_VERSION}-slim
# add bash for entrypoing and python2 for google-cloud-sdk
RUN apt-get update -qqy && apt-get install -qqy bash python
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=0 /usr/local /usr/local
WORKDIR /app
COPY .bigqueryrc /root/
COPY . .
ENTRYPOINT ["/app/entrypoint"]