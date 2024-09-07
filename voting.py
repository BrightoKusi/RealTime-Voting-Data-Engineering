import psycopg2 
import logging
import json
import random
import datetime
import time
from confluent_kafka import Consumer, KafkaError, SerializingProducer
from main import delivery_report

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Initialize Kafka consumer and producer
consumer = Consumer(conf | {
        'group.id': 'voter_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_operations.log'),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect("host=localhost dbname=voting_db user=postgres password=postgres")
        cur = conn.cursor()

        # Fetch candidates from the database
        cur.execute("""
            SELECT row_to_json(col)
            FROM (SELECT * FROM candidates) col;
        """)
        candidates = [candidate[0] for candidate in cur.fetchall()]

        if len(candidates) == 0:
            logging.warning("No candidate found in the database")
        else:
            logging.info(f"Fetched {len(candidates)} candidates from the database")

        # Subscribe to Kafka topic
        consumer.subscribe(['voters_topic'])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                        break
                else:
                    voter = json.loads(msg.value().decode('utf-8'))
                    chosen_candidate = random.choice(candidates)
                    vote = {
                        **voter,
                        **chosen_candidate,
                        'voting_time': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                        'vote': 1
                    }

                    try:
                        # Insert vote into the database
                        cur.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time, vote)
                            VALUES (%s, %s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'], vote['vote']))
                        conn.commit()

                        # Produce vote data to Kafka
                        producer.produce(
                            topic='votes_topic',
                            key=vote['voter_id'],
                            value=json.dumps(vote),
                            on_delivery=delivery_report
                        )
                        producer.poll(0)
                        logging.info(f"Produced vote data for {vote['voter_id']}")
                        time.sleep(0.5)

                    except Exception as e:
                        logging.error(f"Couldn't insert values into votes database due to: {e}")
        
        except Exception as e:
            logging.error(f"Could not execute Kafka consumer loop due to: {e}")
        finally:
            consumer.close()

    except psycopg2.Error as e:
        logging.error(f"Couldn't connect to PostgreSQL due to {e}")
    
    finally:
        # Close cursor and connection to ensure resources are freed
        if cur:
            cur.close()
        if conn:
            conn.close()
