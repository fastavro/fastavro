FROM python:3.5

RUN apt-get update && apt-get install gcc python3-dev -y
RUN pip install pytest flake8 check-manifest
RUN apt-get install libsnappy-dev -y && \
    pip install snappy
RUN pip install python-snappy

COPY . /app/fastavro
WORKDIR /app/fastavro

RUN sed -i -e 's/\r//' ./run-tests.sh
RUN /bin/bash ./run-tests.sh