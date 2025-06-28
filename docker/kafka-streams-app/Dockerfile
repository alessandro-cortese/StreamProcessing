# Usa un'immagine base Ubuntu
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    maven \
    wget \
    curl \
    net-tools \
    iputils-ping \
    unzip \
    git \
    nano \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY ./start_producer.sh /app/start_producer.sh
COPY ./start_consumer.sh /app/start_consumer.sh

RUN chmod +x /app/start_producer.sh /app/start_consumer.sh

