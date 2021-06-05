import json
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)

producer = KafkaProducer(bootstrap_servers=['<host1>:<port>', '<host2>:<port>', '<host3>:<port>'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

admin = KafkaAdminClient(
    bootstrap_servers=['<host1>:<port>', '<host2>:<port>', '<host3>:<port>'], 
    client_id='admin'
)

new_topic = NewTopic('all_events', num_partitions=1, replication_factor=3)
admin.create_topics(new_topics=[new_topic], validate_only=False)

for line in r.iter_lines():
    if line:
        decoded_line = line.decode('utf-8')
        data = json.loads(decoded_line)

        producer.send('all_events', value=data, partition=0)
