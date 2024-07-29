from kafka import KafkaConsumer
import json
# Remove unnecessary imports for text processing
# from mlxtend.preprocessing import TransactionEncoder
# from mlxtend.frequent_patterns import apriori, association_rules
# import pandas as pd
# from collections import deque
# from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
# import string
# import nltk

def create_consumer():
    return KafkaConsumer(
        'product_info_topic',  # Subscribe to the product_info topic
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Read from the beginning of the topic
        enable_auto_commit=True,
        group_id='persistent-group',  # Use a persistent group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


def consume_data(consumer):
    print("Starting consumer...")
    try:
        for message in consumer:
            # Extract title from the message (or use an empty string if not found)
            title = message.value.get('title', '')

            # Skip processing if title is empty (prevents potential crashes)
            if not title:
                print("Skipping processing: Empty title in message.")
                continue  # Skip to the next message

            # Print only the received title (relevant information)
            print(f"Received title: {title}")

    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    # No need for silent downloads as processing is disabled
    # nltk.download('punkt', quiet=True)  # Download NLTK resources silently
    # nltk.download('stopwords', quiet=True)
    consumer = create_consumer()
    consume_data(consumer)

