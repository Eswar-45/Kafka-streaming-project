from confluent_kafka import Producer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

products = [
    ("Laptop", "Electronics", 75000),
    ("Phone", "Electronics", 45000),
    ("Shoes", "Fashion", 3500),
    ("Watch", "Accessories", 12000),
    ("Headphones", "Electronics", 5000),
]

while True:

    product, category, price = random.choice(products)

    order = {
        "order_id": random.randint(100000,999999),
        "customer_id": random.randint(1000,9999),
        "customer_name": fake.name(),
        "city": fake.city(),
        "product": product,
        "category": category,
        "price": price,
        "quantity": random.randint(1,5),
        "event_time": datetime.now().isoformat()
    }

    producer.produce(
        "order-events",
        key=str(order["customer_id"]),
        value=json.dumps(order)
    )

    producer.poll(0)

    print(order)

    time.sleep(1)