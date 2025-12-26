import json
import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def stream_yelp_data(file_path):
    # Connect to Kafka (Using the config from your docker-compose)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], # Or 'localhost:9092' if running outside docker
        value_serializer=json_serializer
    )

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            record = json.loads(line)
            
            # Send to 'customers_review' topic (This acts as the Buffer)
            producer.send('customers_review', record)
            
            print(f"Sent: {record['review_id']}")
            time.sleep(0.01) # Simulate real-time streaming

if __name__ == "__main__":
    stream_yelp_data("datasets/yelp_dataset/yelp_academic_dataset_review.json")