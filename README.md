# NYT-Flink

Applicazione Flink per il processamento distribuito di dati streaming riguardanti commenti del New York Times. 

## Getting Started

### Prerequisites

* [Apache Flink](https://flink.apache.org/)
* [Apache Kafka](https://kafka.apache.org/)
* [Apache Zookeper](https://zookeeper.apache.org/)
* [Redis](https://redis.io/)
* [Python](https://www.python.org/)
* Python libraries: [kafka-python](https://pypi.org/project/kafka-python/)
* [Docker](https://www.docker.com/) (opzionale)
* [Docker Compose](https://docs.docker.com/compose/) (opzionale)

### Installing

È possibile ottenere l'ambiente di esecuzione installando in locale i framework sopra elencati oppure sfruttando Docker ed il relativo file di composizione dei container [docker-compose.yaml](dist/docker-compose.yaml)
```
NYT-Flink/dist$ docker-compose -d up
```
## Running

### Job submission

Per sottomettere il job al jobmanager di Flink è possibile utilizzare l'interfaccia grafica esposta sulla porta 8081 oppure sfruttare lo script [submit_job.sh](dist/submit_job.sh).
```
$ NYT-Flink$ mvn clean package; makedir dist/data/flink; cp my-flink-project-0.1.jar dist/data/flink
$ NYT-Flink$ ./submit_job.sh
```

### Data Ingestion

Se il sistema non è stato eretto tramite il docker-compose file creare il topic *flink*:
```
KAFKA_HOME$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flink
```

Impostare a piacimento la costante di compressione temporale K ed eseguire il comando 
```
NYT-Flink$ python stream-simulator.py
```

### Shutting Down

Per terminare il sistema:
```
NYT-Flink/dist$ docker-compose down
```

## Authors

* **Aldo Pietrangeli**
* **Simone Falvo**

## Acknowledgments

Librerie Python
* [kafka-python](https://pypi.org/project/kafka-python/)
