# NodeManager Hadoop 3.4.3 (Java 11) + JRE 8 copie depuis l'image Hive.
# Raison : HiveServer2 (Java 8) serialise le plan avec Kryo ; les taches
# MapReduce doivent le relire avec la meme version de Java, sinon
# "KryoException: Unable to resolve reference" dans reduce.xml.
FROM apache/hive:4.0.0 AS hive

FROM apache/hadoop:3.4.3
COPY --from=hive /usr/local/openjdk-8 /opt/java8

ENV JAVA_HOME=/opt/java8
ENV PATH=/opt/java8/bin:${PATH}