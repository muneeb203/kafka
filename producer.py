import json
from kafka import KafkaProducer
from time import sleep

# Configuration for Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Update if your Kafka is hosted elsewhere
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serializer for JSON data
)

# Define topics
topics = {
    'product_info': 'product_info_topic'
}

# Function to preprocess data
def preprocess_data(data_item):
    return {
        'asin': data_item.get('asin', ''),
        
        'title': data_item.get('title', '')
    }

def send_data_to_kafka(data_generator):
    for data in data_generator:
        # Print entire data row
        print(json.dumps(data, indent=4))

        # Send 'asin', 'categories', and 'title' to the product_info topic
        producer.send(topics['product_info'], {
            'asin': data.get('asin', ''),
           
            'title': data.get('title', '')
        })

        producer.flush()  # Ensure all data is sent
        sleep(1)  # Simulate real-time streaming by waiting between sends

def load_and_process_json(filename):
    with open(filename, 'r') as file:
        try:
            data = json.load(file)
            for item in data:
                yield preprocess_data(item)
        except json.JSONDecodeError:
            file.seek(0)
            for line in file:
                if line.strip():
                    data_item = json.loads(line)
                    yield preprocess_data(data_item)

# Main function to run the producer
if __name__ == "__main__":
    filename = 'pp_025.json'  # Update to your actual JSON file path
    data_generator = load_and_process_json(filename)
    send_data_to_kafka(data_generator)

