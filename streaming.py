from typing import Dict
import faust
from typing import List, Any

app = faust.App(
    id='streaming',
    broker='kafka://127.0.0.1:9092',
    value_serializer='json',
    topic_partitions=1,
    topic_replication_factor=1,
    topic_disable_leader=True
)

class Order(faust.Record, serializer="json"):
    order_id: str
    source: str
    value: Dict[str, str]

async def on_change(*args, **kwargs):
    print(args, kwargs)

async def on_closed(*args, **kwargs):
    print("+++@", args, kwargs)

table = app.GlobalTable(
    name='agregate', 
    default=list, 
    partitions=1,
    on_changelog_event=on_change,
    on_window_close=on_closed,
    ) \
    .tumbling(2, expires=60*60)\
    .relative_to_stream()


kafka_topic = app.topic('test', value_type=Order, partitions=1, replicas=1)

@app.agent(kafka_topic)
async def process(transactions):
    async for value in transactions:
        table[value.order_id] = table[value.order_id].value() + [value]
        print(value.order_id, table[value.order_id].value())


# conda activate faust &&  cd projects/python-streams/  && faust -A streaming worker -l Info