FROM apache/flink:latest

USER root

ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# IMPROVED INSTALL COMMAND: Added timeout and retries
RUN pip3 install --no-cache-dir --default-timeout=1000 apache-flink

RUN mkdir -p /opt/flink/data && chmod -R 777 /opt/flink/data

USER flink
WORKDIR /opt/flink