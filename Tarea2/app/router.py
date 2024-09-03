from fastapi import APIRouter, HTTPException
from schema import Product, ProductWithStatus
from conf import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC, SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import List
import json
import asyncio
import utils

route = APIRouter()

@route.post('/send')
async def send(product: Product):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        product_id = hash(product.name + product.email) % (10 ** 8)
        product_with_status = ProductWithStatus(
            id=product_id,
            name=product.name,
            price=product.price,
            email=product.email,
            status="recibido"
        )
        value_json = json.dumps(product_with_status.dict()).encode('utf-8')
        await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
    finally:
        await producer.stop()

async def consume():
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    try:
        async for msg in consumer:
            product_with_status = json.loads(msg.value.decode('utf-8'))
            print(f'Consumer msg: {product_with_status}')
            new_status = "procesando" if product_with_status['status'] == "recibido" else "finalizado"
            product_with_status['status'] = new_status

            utils.send_email(
                SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
                product_with_status['email'],
                f"Estado del pedido {product_with_status['id']}",
                f"El estado de su pedido es {product_with_status['status']}"
            )

            if new_status != "finalizado":
                await asyncio.sleep(10) 
                producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
                await producer.start()
                try:
                    value_json = json.dumps(product_with_status).encode('utf-8')
                    await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
                finally:
                    await producer.stop()
    finally:
        await consumer.stop()
