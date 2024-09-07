import psycopg2
import logging
import requests
import random
import json
from confluent_kafka import SerializingProducer

# Initialize Kafka producer with configuration
producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
# Base URL for fetching random user data
BASE_URL = "https://randomuser.me/api/?nat=gb"
# List of party affiliations
PARTIES = ["Management_Party", "Saviour_Party", "Tech_Republic_Party"]

# Configure logging to output messages to both file and console
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_operations.log'),
        logging.StreamHandler()
    ]
)

# Seed the random number generator for reproducibility
random.seed(21)

def create_tables(conn, cur):
    """
    Create tables in the PostgreSQL database if they do not already exist.
    """
    # Create candidates table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(225) PRIMARY KEY,
            candidate_name VARCHAR(225),
            party_affiliation VARCHAR(225),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
        """)
    logging.info("Table 'candidates' created or already exists.")

    # Create voters table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),  -- Added address_city
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
        """)
    logging.info("Table 'voters' created or already exists.")

    # Create votes table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255),
            candidate_id VARCHAR(255),
            voting_time VARCHAR(255),
            vote INT DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
        """)
    logging.info("Table 'votes' created or already exists.")

    conn.commit()

def generate_candidate_data(candidate_number, total_parties):
    """
    Generate random candidate data.
    
    Args:
        candidate_number (int): Number to determine candidate gender.
        total_parties (int): Total number of parties to choose from.
        
    Returns:
        dict: A dictionary containing candidate data.
    """
    # Determine gender based on candidate number
    gender = "female" if candidate_number % 2 == 1 else "male"
    response = requests.get(BASE_URL + f"&gender={gender}")
    
    if response.status_code == 200:
        user_data = response.json()["results"][0]
        candidate_data = {
            "candidate_id": user_data["login"]["uuid"],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": f"Party {candidate_number % total_parties + 1}",
            "biography": "A brief biography of the candidate",
            "campaign_platform": "Key campaign promises and agenda",
            "photo_url": user_data['picture']['large']
        }
        return candidate_data
    else:
        raise Exception(f"Failed to fetch candidate data due to {response.status_code}")

def insert_candidate_data(conn, cur, candidate_data):
    """
    Insert candidate data into the database.
    
    Args:
        conn (psycopg2.connection): Database connection object.
        cur (psycopg2.cursor): Database cursor object.
        candidate_data (dict): Dictionary containing candidate data.
    """
    try:
        insert_query = """
            INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, 
            biography, campaign_platform, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            candidate_data["candidate_id"],
            candidate_data["candidate_name"],
            candidate_data['party_affiliation'],
            candidate_data['biography'],
            candidate_data['campaign_platform'],
            candidate_data['photo_url']
        ))
        conn.commit()
        logging.info("Data inserted for candidate")
    except psycopg2.Error as e:
        logging.error(f"Couldn't insert data due to: {e}")
        conn.rollback()

def generate_voter_data():
    """
    Generate random voter data.
    
    Returns:
        dict: A dictionary containing voter data.
    """
    response = requests.get(BASE_URL)
    
    if response.status_code == 200:
        user_data = response.json()["results"][0]
        voter_data = {
            "voter_id": user_data["login"]["uuid"],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data["dob"]["date"].split("T")[0], 
            "gender": user_data["gender"],
            "nationality": user_data["nat"],
            "registration_number": user_data["id"]["value"],
            "address_street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
            "address_city": user_data["location"]["city"], 
            "address_state": user_data["location"]["state"],
            "address_country": user_data["location"]["country"],
            "address_postcode": user_data["location"]["postcode"],
            "email": user_data["email"],
            "phone_number": user_data["phone"],
            "picture": user_data["picture"]["large"],
            "registered_age": user_data["registered"]["age"]
        }
        return voter_data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def insert_voter_data(conn, cur, voter_data):
    """
    Insert voter data into the database.
    
    Args:
        conn (psycopg2.connection): Database connection object.
        cur (psycopg2.cursor): Database cursor object.
        voter_data (dict): Dictionary containing voter data.
    """
    try:
        insert_query = """
            INSERT INTO voters (
                voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
                address_street, address_city, address_state, address_country, address_postcode,
                email, phone_number, picture, registered_age
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            voter_data["voter_id"],
            voter_data["voter_name"],
            voter_data["date_of_birth"],
            voter_data["gender"],
            voter_data["nationality"],
            voter_data["registration_number"],
            voter_data["address_street"],
            voter_data["address_city"],  
            voter_data["address_state"],
            voter_data["address_country"],
            voter_data["address_postcode"],
            voter_data["email"],
            voter_data["phone_number"],
            voter_data["picture"],
            voter_data["registered_age"]
        ))
        conn.commit()
        logging.info("Data inserted for voter")
    except psycopg2.Error as e:
        logging.error(f"Couldn't insert data due to: {e}")
        conn.rollback()

def produce_voterData_to_kafka(voter_data):
    """
    Produce voter data to a Kafka topic.
    
    Args:
        voter_data (dict): Dictionary containing voter data.
    """
    producer.produce(
        topic="voters_topic",
        key=str(voter_data["voter_id"]),
        value=json.dumps(voter_data),
        on_delivery=delivery_report
    )
    logging.info(f"Produced voter data for {voter_data['voter_name']}")

def delivery_report(err, msg):
    """
    Callback function for Kafka message delivery report.
    
    Args:
        err (KafkaError): Error if message delivery failed.
        msg (Message): Kafka message object.
    """
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    """
    Main function to connect to the database, create tables, and insert data.
    """
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect("host=localhost dbname=voting_db user=postgres password=postgres")
        cur = conn.cursor()
        
        # Create tables if they do not exist
        create_tables(conn, cur)

        # Check if there are any candidates already in the database
        cur.execute("SELECT * FROM candidates")
        candidates = cur.fetchall()
        if len(candidates) == 0:
            # Generate and insert candidate data if no candidates exist
            for i in range(3):
                candidate_data = generate_candidate_data(i, 3)
                insert_candidate_data(conn, cur, candidate_data)

        # Generate, insert voter data, and produce to Kafka
        for voter_number in range(1000):
            voter_data = generate_voter_data()
            insert_voter_data(conn, cur, voter_data)  
            produce_voterData_to_kafka(voter_data)
        
        # Ensure all messages are delivered before closing
        producer.flush()
    except psycopg2.Error as e:
        logging.error(f"Unable to create table due to: {e}")
    finally:
        # Close database cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
