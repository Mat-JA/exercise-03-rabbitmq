import os
import time
import json
import sys
import pika

def main():
    rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    queue_name = "node_events"

    # Retry connection to RabbitMQ because it might not be ready instantly
    connection = None
    for i in range(30):
        try:
            params = pika.URLParameters(rabbitmq_url)
            connection = pika.BlockingConnection(params)
            break
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ...", flush=True)
            time.sleep(2)
    
    if not connection:
        print("Could not connect to RabbitMQ, exiting.", flush=True)
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode('utf-8'))
            event = data.get("event")
            node_name = data.get("node_name")
            timestamp = data.get("timestamp")
            print(f"EVENT: {event} | node: {node_name} | time: {timestamp}", flush=True)
        except Exception as e:
            print(f"Error processing message: {e}", flush=True)
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print("Consumer started, waiting for messages...", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
