import json

from kafka import KafkaProducer

topic_name = 'quickstart-events-test'
# produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                         bootstrap_servers=['localhost:9092'])

# produce asynchronously
for _ in range(1, 11):
    data = {'name': f"abu shoaib {_}", "age": 28, "ids": [2, 13, 56, [99, 25]]}
    producer.send(topic_name, value=data)

producer.flush()

print('Message produced successfully')
