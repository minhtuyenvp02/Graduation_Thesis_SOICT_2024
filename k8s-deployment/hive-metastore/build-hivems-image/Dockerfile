FROM openjdk:8-jre

WORKDIR /opt
ENV POSTGRES_CONNECTOR_VERSION=42.5.4
ENV HADOOP_VERSION=3.2.0
ENV METASTORE_VERSION=3.0.0
ENV HIVE_METASTORE_IP=''
ENV HIVE_METASTORE_PORT=''
ENV POSTGRES_USER=''
ENV POSTGRES_PASSWORD=''
ENV POSTGRES_DB_IP=''
ENV POSTGRES_DB_PORT=''
ENV S3_USER=''
ENV S3_PASSWORD=''
ENV S3_END_POINT=''

## SET HIVE and HADOOP PATH
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

# DOWNLOAD HIVE_METASTORE(STANDALONE) AND HADOOP
RUN curl -L https://archive.apache.org/dist/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf -

## DOWNLOAD Delta_Hive Connector
RUN  curl -o delta-hive-assembly_2.12-3.2.0.jar https://github.com/delta-io/connectors/releases/download/v0.2.0/delta-hive-assembly_2.11-0.2.0.jar && \
    cp delta-hive-assembly_2.12-3.2.0.jar ${HIVE_HOME}/lib/ && \
    cp delta-hive-assembly_2.12-3.2.0.jar ${HADOOP_HOME}/share/hadoop/tools/lib/


# Download and install the postgres connector used by HiveMetastore
RUN curl -s https://jdbc.postgresql.org/download/postgresql-42.5.4.jar -o postgresql-$POSTGRES_CONNECTOR_VERSION.jar && mv postgresql-$POSTGRES_CONNECTOR_VERSION.jar /opt/postgresql-jdbc.jar
RUN cp /opt/postgresql-jdbc.jar  ${HADOOP_HOME}/share/hadoop/tools/lib/ && chmod -R 775 ${HADOOP_HOME}/share/hadoop/common/lib/*
RUN cp /opt/postgresql-jdbc.jar ${HIVE_HOME}/lib/ && chmod -R 775 ${HIVE_HOME}/lib/*


## Move Hive_metastore configuration
COPY metastore-site.xml ${HIVE_HOME}/conf
## Move the Entrypoint file
COPY entrypoint.sh /entrypoint.sh

## Updating permission to run the entrypoint file
RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

USER hive
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]