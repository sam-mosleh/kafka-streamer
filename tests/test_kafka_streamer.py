# import asyncio
# import os

# import pytest
# from kafka_streamer.consumer import KafkaConsumer
# from kafka_streamer.models import KafkaResponse
# from kafka_streamer.producer import KafkaProducer
# from kafka_streamer.streamer import KafkaStreamer

# host = os.environ.get("KAFKA_HOST")
# topic = os.environ.get("KAFKA_TOPIC")
# group_id = os.environ.get("KAFKA_GROUPID")
# data = os.environ.get("KAFKA_DATA").encode("utf8")

# async def simple_produce():
#     async with KafkaProducer(host) as producer:
#         await producer.produce(topic, data)

# async def produce_from_queue():
#     q = asyncio.Queue()
#     producer = KafkaProducer(host)
#     asyncio.create_task(producer.queue_to_kafka(q))
#     for _ in range(100):
#         await q.put(KafkaResponse(topic, value=data))

# async def simple_consume():
#     async with KafkaConsumer(host, group_id, [topic]) as consumer:
#         message = await consumer.fetch()
#     return message

# async def consume_to_queue():
#     q = asyncio.Queue(maxsize=1)
#     consumer = KafkaConsumer(host, group_id, [topic])
#     asyncio.create_task(consumer.kafka_to_queue(q))
#     return await q.get()

# @pytest.mark.timeout(10)
# def test_simple_produce():
#     asyncio.run(simple_produce())

# @pytest.mark.timeout(10)
# def test_simple_consume():
#     asyncio.run(simple_consume())

# @pytest.mark.timeout(10)
# def test_consumer_to_queue():
#     asyncio.run(consume_to_queue())

# @pytest.mark.timeout(10)
# def test_produce_from_queue():
#     asyncio.run(produce_from_queue())

# @pytest.mark.timeout(10)
# def test_streamer():
#     streamer = KafkaStreamer(host, group_id)

#     @streamer.topic(topic)
#     def rlc_consumer(msg):
#         raise Exception

#     with pytest.raises(Exception):
#         asyncio.run(streamer.run())
