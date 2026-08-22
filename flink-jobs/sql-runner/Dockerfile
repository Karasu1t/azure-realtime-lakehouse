FROM flink:1.18.1-java17

# Enables abfss:// paths so checkpoints can be written to ADLS2 instead of
# the JobManager Pod's local disk. Flink's official images ship this plugin
# jar under opt/, but plugins are only loaded from plugins/ -- verify the
# exact jar filename against the installed Flink version if this breaks.
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop && \
    cp /opt/flink/opt/flink-azure-fs-hadoop-*.jar /opt/flink/plugins/azure-fs-hadoop/

COPY target/sql-runner.jar /opt/flink/usrlib/sql-runner.jar
