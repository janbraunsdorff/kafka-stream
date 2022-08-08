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

#changelog = app.topic('streaming-agregate-changelog', partitions=1, replicas=1)
repartiotion = app.topic('streaming.process-test-Order.type-repartition', partitions=1, replicas=1)


# @app.agent(changelog)
# async def process(transactions):
#     async for value in transactions:
#         print("change", value, type(value))


@app.agent(repartiotion)
async def process(transactions):
    async for value in transactions:
        print("repartiotion", value, type(value))