from confluent_kafka import Producer
import random
import time
from avro_helper import load_schema, serialize_avro

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'orders'

schema = load_schema()
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def create_order():
    """Generate random order"""
    return {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]),
        "price": round(random.uniform(50.0, 1500.0), 2)
    }

def delivery_callback(err, msg):
    """Callback for message delivery"""
    if err:
        print(f"X - Delivery failed: {err}")
    else:
        print(f"√ - Delivered to {msg.topic()} [partition {msg.partition()}]")

if __name__ == "__main__":
    print("○ Starting Producer...")
    print("=" * 60)
    
    try:
        while True:
            order = create_order()
            avro_bytes = serialize_avro(order, schema)
            
            producer.produce(
                topic=TOPIC,
                value=avro_bytes,
                callback=delivery_callback
            )
            producer.poll(0)
            
            print(f"→ Sent: Order {order['orderId']} | {order['product']} | ${order['price']}")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n► Shutting down producer...")
    finally:
        producer.flush()