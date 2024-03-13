FROM python:3

WORKDIR /app

ADD pyproject.toml /app/pyproject.toml

RUN pip install --upgrade pip
