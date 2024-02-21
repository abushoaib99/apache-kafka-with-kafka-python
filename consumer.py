import json

from kafka import KafkaConsumer

topic_name = 'quickstart-events-test'
# To consume latest messages and auto-commit offsets
# consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'])

# consume json messages
json_consumer = KafkaConsumer(topic_name,
                              value_deserializer=lambda m: json.loads(m.decode('ascii')),
                              bootstrap_servers=['localhost:9092'],
                              )

print("Waiting for Message")
for message in json_consumer:
    value = message.value
    print("message:: ", value, type(value))

# Close consumer
json_consumer.close()
