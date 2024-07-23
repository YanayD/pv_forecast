FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN mkdir -p /root/.cache/pip

RUN pip install --cache-dir /root/.cache/pip -r requirements.txt