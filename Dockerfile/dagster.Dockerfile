FROM python:3.11

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ADD /dagster_service/ /app/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
