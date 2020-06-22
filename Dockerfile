FROM debian:latest

RUN apt-get update &&  apt-get install -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
    python3 python3-pip python-pip python3.5 python 3.6 git

RUN pip install virtualenv

COPY . /source
WORKDIR /source
RUN sed -i -e 's/\r//' /source/run-tests.sh

RUN virtualenv -p python3.5 testenv3 \
    && . testenv3/bin/activate \
    && pip install -r /source/developer_requirements.txt \
    && /bin/bash /source/run-tests.sh
