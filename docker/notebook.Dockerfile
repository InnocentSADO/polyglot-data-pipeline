FROM apache/hadoop:3.4.3 AS hadoop
FROM apache/hive:4.0.0 AS hive

FROM quay.io/jupyter/pyspark-notebook@sha256:a9835c9be3a64ba81e090a1c996b51c07500b5537f555bd3f5b9d15368a776b3

USER root
COPY --from=hadoop /opt/hadoop /opt/hadoop
COPY --from=hive /opt/hive /opt/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/hive/bin:${PATH}
RUN pip install --no-cache-dir pymongo==4.11.3 cassandra-driver==3.29.3 requests==2.32.3
USER ${NB_UID}
WORKDIR /home/jovyan/work
