import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import avro.schema
import avro.io
import io
import config


class OrderProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=self._avro_serializer,
            acks='all',
            retries=3
        )
        
        with open('schema/order.avsc', 'r') as schema_file:
            self.schema = avro.schema.parse(schema_file.read())
        
        print("Producer initialized successfully")
        print(f"Will produce {config.TOTAL_ORDERS_TO_PRODUCE} orders to topic: {config.ORDERS_TOPIC}")
        print(f"Interval: {config.PRODUCER_INTERVAL_SECONDS} seconds\n")

    def _avro_serializer(self, order_data):
        writer = avro.io.DatumWriter(self.schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(order_data, encoder)
        return bytes_writer.getvalue()

    def generate_order(self, order_id):
        product = random.choice(config.PRODUCTS)
        price = round(random.uniform(config.MIN_PRICE, config.MAX_PRICE), 2)
        
        order = {
            "orderId": str(order_id),
            "product": product,
            "price": price
        }
        return order

    def send_order(self, order):
        try:
            future = self.producer.send(config.ORDERS_TOPIC, value=order)
            record_metadata = future.get(timeout=10)
            
            print(f"Sent: Order #{order['orderId']} | "
                  f"Product: {order['product']:<12} | "
                  f"Price: ${order['price']:>7.2f} | "
                  f"Partition: {record_metadata.partition}")
            
            return True
        except KafkaError as e:
            print(f"Failed to send order {order['orderId']}: {e}")
            return False

    def produce_orders(self):
        print("Starting order production...\n")
        
        for i in range(1, config.TOTAL_ORDERS_TO_PRODUCE + 1):
            order = self.generate_order(i)
            self.send_order(order)
            
            if i < config.TOTAL_ORDERS_TO_PRODUCE:
                time.sleep(config.PRODUCER_INTERVAL_SECONDS)
        
        print(f"\nSuccessfully produced {config.TOTAL_ORDERS_TO_PRODUCE} orders")
        self.producer.flush()
        self.producer.close()


def main():
    try:
        producer = OrderProducer()
        producer.produce_orders()
    except KeyboardInterrupt:
        print("\nProduction interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()