from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

string = ''
while (string != 'exit'):
    # input
    string = str(input())

    producer.send('foo', str.encode(string))

producer.flush()
