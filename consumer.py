import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import avro.schema
import avro.io
import io
import config


class OrderConsumer:
    def __init__(self):
        # Main consumer
        self.consumer = KafkaConsumer(
            config.ORDERS_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=self._avro_deserializer
        )
        
        # Retry consumer
        self.retry_consumer = KafkaConsumer(
            config.ORDERS_RETRY_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.CONSUMER_GROUP_ID + '-retry',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Producer for retry and DLQ
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        with open('schema/order.avsc', 'r') as schema_file:
            self.schema = avro.schema.parse(schema_file.read())
        
        self.total_processed = 0
        self.total_price = 0.0
        self.retry_count = 0
        self.dlq_count = 0
        
        print("Consumer initialized successfully")
        print(f"Consuming from topic: {config.ORDERS_TOPIC}")
        print(f"Retry topic: {config.ORDERS_RETRY_TOPIC}")
        print(f"DLQ topic: {config.ORDERS_DLQ_TOPIC}\n")

    def _avro_deserializer(self, msg_value):
        bytes_reader = io.BytesIO(msg_value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        return reader.read(decoder)

    def process_order(self, order):
        order_id = order['orderId']
        product = order['product']
        price = order['price']
        
        if price > config.PERMANENT_FAILURE_THRESHOLD:
            print(f"FRAUD ALERT: Order #{order_id} - Price ${price:.2f} exceeds limit!")
            return False, False
        
        if price < config.TEMP_FAILURE_THRESHOLD:
            print(f"TEMP FAILURE: Order #{order_id} - Price ${price:.2f} too low, needs retry")
            return False, True 
        
        self.total_processed += 1
        self.total_price += price
        running_avg = self.total_price / self.total_processed
        
        print(f"Processed: Order #{order_id:<4} | "
              f"Product: {product:<12} | "
              f"Price: ${price:>7.2f} | "
              f"Running Avg: ${running_avg:>7.2f}")
        
        return True, False

    def send_to_retry(self, order, retry_attempt=1):
        retry_message = {
            'order': order,
            'retry_attempt': retry_attempt,
            'original_topic': config.ORDERS_TOPIC
        }
        
        try:
            self.producer.send(config.ORDERS_RETRY_TOPIC, value=retry_message)
            self.retry_count += 1
            print(f"Sent to RETRY: Order #{order['orderId']} (Attempt {retry_attempt}/{config.MAX_RETRIES})")
        except KafkaError as e:
            print(f"Failed to send to retry: {e}")

    def send_to_dlq(self, order, reason="Unknown"):
        dlq_message = {
            'order': order,
            'reason': reason,
            'timestamp': time.time()
        }
        
        try:
            self.producer.send(config.ORDERS_DLQ_TOPIC, value=dlq_message)
            self.dlq_count += 1
            print(f"Sent to DLQ: Order #{order['orderId']} | Reason: {reason}")
        except KafkaError as e:
            print(f"Failed to send to DLQ: {e}")

    def handle_retry_message(self, retry_message):
        order = retry_message['order']
        retry_attempt = retry_message['retry_attempt']
        
        print(f"\nProcessing RETRY: Order #{order['orderId']} (Attempt {retry_attempt})")
        
        # Simulate retry delay
        time.sleep(config.RETRY_DELAY_SECONDS)
        
        success, should_retry = self.process_order(order)
        
        if not success:
            if should_retry and retry_attempt < config.MAX_RETRIES:
                # Retry again
                self.send_to_retry(order, retry_attempt + 1)
            else:
                # Max retries reached or permanent failure
                reason = "Max retries exceeded" if should_retry else "Permanent failure"
                self.send_to_dlq(order, reason)

    def consume_orders(self):
        print("Starting order consumption...\n")
        
        try:
            while True:
                main_messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                for topic_partition, messages in main_messages.items():
                    for message in messages:
                        order = message.value
                        success, should_retry = self.process_order(order)
                        
                        if not success:
                            if should_retry:
                                self.send_to_retry(order)
                            else:
                                self.send_to_dlq(order, "Permanent failure")
                
                retry_messages = self.retry_consumer.poll(timeout_ms=1000, max_records=5)
                for topic_partition, messages in retry_messages.items():
                    for message in messages:
                        self.handle_retry_message(message.value)
                
        except KeyboardInterrupt:
            print("\nConsumption interrupted by user")
            self.print_statistics()
        finally:
            self.consumer.close()
            self.retry_consumer.close()
            self.producer.close()

    def print_statistics(self):
        print("\n" + "="*60)
        print("FINAL STATISTICS")
        print("="*60)
        print(f"Successfully processed: {self.total_processed} orders")
        if self.total_processed > 0:
            print(f"Average price: ${self.total_price / self.total_processed:.2f}")
        print(f"Retried orders: {self.retry_count}")
        print(f"DLQ orders: {self.dlq_count}")
        print("="*60)


def main():
    try:
        consumer = OrderConsumer()
        consumer.consume_orders()
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()