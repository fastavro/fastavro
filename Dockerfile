FROM python:3.9-buster

RUN apt-get update && apt-get install -y libsnappy-dev && pip install python-snappy

COPY ./developer_requirements.txt /developer_requirements.txt
RUN pip install -r /developer_requirements.txt

COPY . /source
WORKDIR /source
RUN sed -i -e 's/\r//' /source/run-tests.sh

RUN /bin/bash /source/run-tests.sh
