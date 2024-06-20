FROM python:3.11

WORKDIR /app

ADD /dagster_service/ /app/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
