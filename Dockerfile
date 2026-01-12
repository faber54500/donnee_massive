FROM debian:11-slim

USER root

# 1. Installation de Java et des outils
RUN apt-get update && apt-get install -y openjdk-11-jdk python3 wget && apt-get clean

# 2. Installation de Hadoop 3.3.6
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && rm hadoop-3.3.6.tar.gz

# 3. Installation de Spark 3.3.1
RUN wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.1-bin-hadoop3.tgz -C /opt/ && rm spark-3.3.1-bin-hadoop3.tgz

# 4. Variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV SPARK_HOME=/opt/spark-3.3.1-bin-hadoop3
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

# 5. CONFIGURATION HDFS (C'est ça qui règle ton problème !)
RUN echo '<?xml version="1.0"?><configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property></configuration>' > $HADOOP_HOME/etc/hadoop/core-site.xml
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

WORKDIR /opt