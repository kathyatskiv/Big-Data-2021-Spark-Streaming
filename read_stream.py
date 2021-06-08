import json
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from config import HOST_1, HOST_2, HOST_3

r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)

producer = KafkaProducer(bootstrap_servers=[HOST_1, HOST_2, HOST_3],
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

admin = KafkaAdminClient(
    bootstrap_servers=[HOST_1, HOST_2, HOST_3], 
    client_id='admin'
)


for line in r.iter_lines():
    if line:
        decoded_line = line.decode('utf-8')
        data = json.loads(decoded_line)

        producer.send('all_events', value=data, partition=0)
