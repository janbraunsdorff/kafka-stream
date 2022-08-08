from confluent_kafka import Producer
import uuid
import json
bootstrap_servers = "localhost:9092"


def confluent_kafka_producer():

    p = Producer({'bootstrap.servers': bootstrap_servers})
    record_key = str(uuid.uuid4())
    base_message_A = {
        "order_id": 'abc',
        "source": "A",
        "value": {
            "A": "1",
            "B": "2"
        }
    }

    base_message_B = {
        "order_id": '2abc',
        "source": "B",
        "value": {
            "C": "1",
            "D": "2"
        }
    }
    record_value = json.dumps(base_message_A)
    p.produce("test", key=record_key, value=record_value)

    record_value = json.dumps(base_message_B)
    p.produce("test", key=record_key, value=record_value)

    p.poll(0)

    p.flush()
    print("send completed")

confluent_kafka_producer()