FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu

USER root

# Upgrade to Python 3.12 to match Airflow
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-dev python3.12-venv && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    update-alternatives --set python3 /usr/bin/python3.12 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12 && \
    rm -rf /var/lib/apt/lists/*

# Downloads S3A connector JARs so spark can read from minio
RUN curl -fL --progress-bar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL --progress-bar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Download Iceberg Spark runtime JAR
RUN curl -fL --progress-bar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

# Download AWS SDK v2 JARs (required by Iceberg S3FileIO on executors)
RUN curl -fL --progress-bar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-v2-2.20.18.jar && \
    curl -fL --progress-bar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar \
    -o /opt/spark/jars/url-connection-client-2.20.18.jar

# Install the pipeline package and clickhouse-driver (pip already installed with Python 3.12)
# clickhouse-driver is needed on executors for write_dataframe_to_clickhouse foreachPartition
COPY pyproject.toml /opt/pipeline/
COPY src/ /opt/pipeline/src/
RUN pip3 install --no-cache-dir /opt/pipeline/[spark_dockerfile]

# Copy spark jobs
COPY spark/ /opt/spark/jobs/
