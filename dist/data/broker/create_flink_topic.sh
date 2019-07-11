#!/bin/bash

kafka-topics --create --topic flink --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
