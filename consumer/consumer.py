from confluent_kafka import Consumer, Producer
from avro_helper import load_schema, deserialize_avro, serialize_avro
import time

KAFKA_BROKER = 'localhost:9092'
ORDERS_TOPIC = 'orders'
RETRY_TOPIC = 'orders_retry'
DLQ_TOPIC = 'orders_dlq'
MAX_RETRIES = 3

schema = load_schema()

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
})

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# State tracking
total_price = 0.0
message_count = 0
retry_tracker = {}

def calculate_running_average(price):
    """Calculate running average of prices"""
    global total_price, message_count
    total_price += price
    message_count += 1
    return total_price / message_count

def send_to_retry(order):
    """Send failed message to retry topic"""
    from avro_helper import serialize_avro
    avro_bytes = serialize_avro(order, schema)
    producer.produce(RETRY_TOPIC, value=avro_bytes)
    producer.flush()
    print(f"! - Sent to RETRY: Order {order['orderId']}")

def send_to_dlq(order):
    """Send permanently failed message to DLQ"""
    from avro_helper import serialize_avro
    avro_bytes = serialize_avro(order, schema)
    producer.produce(DLQ_TOPIC, value=avro_bytes)
    producer.flush()
    print(f"◘  Sent to DLQ: Order {order['orderId']}")

def process_order(order, is_retry=False):
    """Process order with retry logic"""
    order_id = order['orderId']
    price = order['price']
    
    # Simulate failure for expensive items (price > 1000)
    if price > 1000:
        print(f"X - Processing FAILED: Order {order_id} | ${price} (too expensive)")
        
        if is_retry:
            # Track retries
            retry_tracker[order_id] = retry_tracker.get(order_id, 0) + 1
            
            if retry_tracker[order_id] >= MAX_RETRIES:
                send_to_dlq(order)
                del retry_tracker[order_id]
                return False
            
            # Wait before retry
            time.sleep(1)
            send_to_retry(order)
            return False
        else:
            # First failure - send to retry
            retry_tracker[order_id] = 0
            send_to_retry(order)
            return False
    
    # Success - calculate running average
    avg = calculate_running_average(price)
    print(f"√ - Processed: Order {order_id} | {order['product']} | ${price:.2f}")
    print(f"    → Running Average: ${avg:.2f} | Total Orders: {message_count}")
    
    # Clean up retry tracker when success
    if order_id in retry_tracker:
        del retry_tracker[order_id]
    
    return True

if __name__ == "__main__":
    print("○ Starting Consumer...")
    print("=" * 60)
    
    consumer.subscribe([ORDERS_TOPIC, RETRY_TOPIC])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"! - Error: {msg.error()}")
                continue
            
            try:
                order = deserialize_avro(msg.value(), schema)
                is_retry = (msg.topic() == RETRY_TOPIC)
                
                if is_retry:
                    print(f"\n→ Retrying Order {order['orderId']}...")
                
                process_order(order, is_retry)
                print("-" * 60)
                
            except Exception as e:
                print(f"X - Failed to process message: {e}")
    
    except KeyboardInterrupt:
        print("\n► Shutting down consumer...")
    
    finally:
        consumer.close()


