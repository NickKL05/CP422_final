# CP422 local Hadoop stack: one image, with the role picked by the container command.
# AI citation: Claude Code (Claude Opus 5.5, Anthropic) drafted this Dockerfile
# (version choices, download steps, env layout). Reviewed, built and tested by Member 1.

FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG PIG_VERSION=0.17.0
ARG HBASE_VERSION=2.5.10
ARG SPARK_VERSION=3.5.3
ARG APACHE_MIRROR=https://archive.apache.org/dist

# OS tools: python3 runs the Hadoop Streaming mappers; nc is used for start-up waits.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 curl ca-certificates procps netcat-openbsd unzip \
 && rm -rf /var/lib/apt/lists/*

# Apache tarballs, unpacked under /opt with docs stripped to keep the image smaller.
RUN curl -fsSL "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" | tar -xz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && rm -rf /opt/hadoop/share/doc
RUN curl -fsSL "${APACHE_MIRROR}/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" | tar -xz -C /opt \
 && mv /opt/pig-${PIG_VERSION} /opt/pig && rm -rf /opt/pig/docs /opt/pig/src /opt/pig/test
RUN curl -fsSL "${APACHE_MIRROR}/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" | tar -xz -C /opt \
 && mv /opt/hbase-${HBASE_VERSION}-hadoop3 /opt/hbase && rm -rf /opt/hbase/docs
RUN curl -fsSL "${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" | tar -xz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV HADOOP_VERSION=${HADOOP_VERSION} \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_PID_DIR=/tmp/pids \
    PIG_HOME=/opt/pig \
    PIG_CLASSPATH=/opt/hadoop/etc/hadoop \
    HBASE_HOME=/opt/hbase \
    HBASE_PID_DIR=/tmp/pids \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    MAPRED_HISTORYSERVER_USER=root
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PIG_HOME}/bin:${HBASE_HOME}/bin:${SPARK_HOME}/bin:${PATH}

# Cluster configuration (block size, replication, YARN sizing, and so on).
COPY conf/hadoop/ ${HADOOP_CONF_DIR}/
COPY conf/hbase/hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY conf/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh

# JAVA_HOME has to be in hadoop-env.sh for the daemons. HBase uses its own Hadoop jars
# (to avoid classpath clashes) but reads our hdfs-site.xml, so it picks up replication and block size.
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh \
 && printf 'export JAVA_HOME=%s\nexport HBASE_CLASSPATH=%s\nexport HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true\nexport HBASE_MANAGES_ZK=true\nexport HBASE_HEAPSIZE=1G\n' \
      "${JAVA_HOME}" "${HADOOP_CONF_DIR}" >> ${HBASE_HOME}/conf/hbase-env.sh \
 && sed -i 's/\r$//' /entrypoint.sh

WORKDIR /opt/cp422
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
CMD ["client"]
