# Use the official Apache Airflow 2.3.0 image as the base image
FROM apache/airflow:2.3.0

# switch to root user
USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# switch to airflow user for rest of the installations
USER airflow

# install requirements.txt for python packages
COPY requirements.txt /requirements.txt
Run pip install --no-cache-dir --user -r /requirements.txt
# copy the dags folder into airflow user's dags directory
COPY --chown=airflow:root ./dags /opt/airflow/dags