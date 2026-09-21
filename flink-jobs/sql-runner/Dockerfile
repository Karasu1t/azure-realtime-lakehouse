FROM flink:1.20.5-java17

ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.10.2
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20

# The stock Flink image ships neither the Kafka SQL connector nor Iceberg, so
# 'connector' = 'kafka' and 'type' = 'iceberg' in the SQL files would fail
# with "Could not find any factory". The Azure bundle provides ADLSFileIO.
ADD ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/
ADD ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD ${MAVEN}/org/apache/iceberg/iceberg-azure-bundle/${ICEBERG_VERSION}/iceberg-azure-bundle-${ICEBERG_VERSION}.jar /opt/flink/lib/

# Enables abfss:// paths so checkpoints can be written to ADLS2 instead of
# the JobManager Pod's local disk. Flink's official images ship this plugin
# jar under opt/, but plugins are only loaded from plugins/ -- verify the
# exact jar filename against the installed Flink version if this breaks.
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop && \
    cp /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/plugins/azure-fs-hadoop/

COPY target/sql-runner.jar /opt/flink/usrlib/sql-runner.jar
