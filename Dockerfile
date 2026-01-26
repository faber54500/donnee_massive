FROM debian:11-slim

USER root

# 1. Installation de Java et des outils
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk python3 wget
RUN apt-get clean

# 2. Installation de Hadoop 3.3.6
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xzf hadoop-3.3.6.tar.gz -C /opt/
RUN rm hadoop-3.3.6.tar.gz

# 3. Installation de Spark 3.3.1
RUN wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
RUN tar -xzf spark-3.3.1-bin-hadoop3.tgz -C /opt/
RUN rm spark-3.3.1-bin-hadoop3.tgz

# 4. Variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV SPARK_HOME=/opt/spark-3.3.1-bin-hadoop3
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

# 5. Configuration
# Donne à Hadoop le carnet d'adresses
COPY core-site.xml /opt/hadoop-3.3.6/etc/hadoop/core-site.xml

# Montre a Hadoop est java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /opt