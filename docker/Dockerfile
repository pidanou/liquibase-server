FROM python:3.10.12-alpine3.18

RUN apk update \
    && apk upgrade \
    && apk add --update openjdk11 tzdata curl unzip bash \
    && apk add git \
    && apk add build-base \
    && rm -rf /var/cache/apk/* 

ENV LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk/lib/server

WORKDIR /app

COPY src .
RUN pip3 install -r requirements.txt
ENTRYPOINT gunicorn -b 0.0.0.0:8000 main:app
EXPOSE 8000