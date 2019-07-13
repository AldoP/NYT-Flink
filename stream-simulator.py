from kafka import KafkaProducer
from time import sleep


if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    #producer = KafkaProducer(bootstrap_servers='35.205.91.60:9092')
    filename = 'data/Comments_jan-apr2018.csv'

    K = 1/4000  # compression factor
    #K = 0  # compression factor

    f = open(filename, "r")

    # read to skip the header
    f.readline()

    line_count = 0
    prev_date = 0
    for line_count, line in enumerate(f):
        date = int(line.split(",")[5])
        if line_count == 0:
            prev_date = date
        sleep((date - prev_date) * K)
        producer.send('flink', str.encode(line))
        prev_date = date
        print(line_count)

    print("Lines proccessed:", line_count)

    f.close()