#!/bin/bash

docker-compose exec broker kafka-topics --describe --zookeeper zookeeper:2181
