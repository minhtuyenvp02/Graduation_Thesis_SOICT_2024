FROM apache/airflow:2.9.1-python3.11
WORKDIR /opt/airflow
ARG ARCH="amd64"
USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${ARCH}

COPY ./requirements.txt ./

RUN pip3 install -r requirements.txt
RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs

COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

#ENTRYPOINT [ "/bin/bash" ]

