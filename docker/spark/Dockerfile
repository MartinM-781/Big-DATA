# Spark 3.5 (Scala 2.12, Java 17, Python 3) with the Kafka connector baked in,
# so that no dependency is resolved at runtime (reproducible, offline-friendly).
FROM spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

ARG SPARK_VERSION=3.5.5
ARG MAVEN=https://repo1.maven.org/maven2

ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV PATH=/opt/spark/bin:$PATH \
    PYTHONPATH=/opt/project/src \
    PYSPARK_PYTHON=python3 \
    DATA_DIR=/data \
    RESULTS_DIR=/opt/project/results

WORKDIR /opt/project
