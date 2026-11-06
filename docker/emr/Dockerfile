
FROM public.ecr.aws/emr-serverless/spark4/emr-spark-8.0.0@sha256:408d7061fa112f2402606eab2a18aa01b5e946549c0249fab708d8e05344ebbe

#snowalke spark connector
ARG SNOWFLAKE_SPARK_VERSION=3.1.1  

#snowflake jdb driver 
ARG SNOWFLAKE_JDBC_VERSION=3.19.0

USER root

ADD --chmod=644 https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/${SNOWFLAKE_SPARK_VERSION}/spark-snowflake_2.13-${SNOWFLAKE_SPARK_VERSION}.jar /usr/lib/spark/jars/
#used to download spark connector HAR into spark library with a readable persimission 644

ADD --chmod=644 https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${SNOWFLAKE_JDBC_VERSION}/snowflake-jdbc-${SNOWFLAKE_JDBC_VERSION}.jar /usr/lib/spark/jars/
#used to download jdbc drver jar into spark library directory

COPY requirements-emr.txt /tmp/requirements-emr.txt

#build a python environmnet
RUN python3.12 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements-emr.txt \
    && rm /tmp/requirements-emr.txt

#configure it with python evironment created 
ENV PYSPARK_PYTHON=/opt/venv/bin/python

USER hadoop:hadoop 
#used to switch from root to hadoop tests runs