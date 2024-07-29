import json
from kafka import KafkaConsumer
from nltk.tokenize import word_tokenize
from collections import deque

# Configuration for Kafka Consumer
consumer = KafkaConsumer(
    'product_info_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-pcy-group',  # Ensure this is unique if you want to start from the beginning
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize hash table size and support threshold
hash_table_size = 10000
support_threshold = 2
decay_factor = 0.95  # Decaying factor to reduce the weight of older transactions

def tokenize_title(title):
    return word_tokenize(title.lower())  # Tokenize the title and convert to lowercase

def pcy_algorithm(transactions):
    item_counts = {}
    hash_table = [0] * hash_table_size

    for transaction in transactions:
        if transaction is None:  # Skip if transaction is None
            continue

        # Apply decay to current counts and hash table
        item_counts = {item: count * decay_factor for item, count in item_counts.items()}
        hash_table = [count * decay_factor for count in hash_table]

        # Count items and populate hash table
        unique_items = set(transaction)
        for item in unique_items:
            item_counts[item] = item_counts.get(item, 0) + 1
            for other_item in unique_items:
                if item != other_item:
                    index = (hash(item) ^ hash(other_item)) % hash_table_size
                    hash_table[index] += 1

    # Determine frequent items and candidate pairs
    frequent_items = {item for item, count in item_counts.items() if count >= support_threshold}
    bitmap = [1 if count >= support_threshold else 0 for count in hash_table]
    candidate_pairs = {frozenset([item1, item2]) for item1 in frequent_items for item2 in frequent_items if item1 != item2 and bitmap[(hash(item1) ^ hash(item2)) % hash_table_size]}

    print("Frequent Items:", frequent_items)
    print("Candidate Pairs:", candidate_pairs)

def consume_data():
    transactions = deque(maxlen=10)  # Sliding window of transactions
    try:
        for message in consumer:
            item = message.value
            title = item.get('title', '')  # Handle missing or None titles
            if title:
                tokenized_title = tokenize_title(title)
                transactions.append(tokenized_title)
                print(f"Processed title: {title}")  # Print the processed title

            if len(transactions) >= 10:
                print(f"Processing batch of {len(transactions)} transactions")
                pcy_algorithm(transactions)
                transactions.clear()  # Clear the batch after processing
    except KeyboardInterrupt:
        print("Stopped consuming")

if __name__ == "__main__":
    consume_data()

