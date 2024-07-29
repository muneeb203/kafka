import json
from kafka import KafkaConsumer
from mlxtend.frequent_patterns import fpgrowth
import pandas as pd

def tokenize_title(title):
    return title.lower().split()  # Tokenize the title and convert to lowercase

def mine_frequent_itemsets(titles):
    # Create a DataFrame with one-hot encoding
    encoded_titles = pd.get_dummies(pd.DataFrame(titles), prefix='', prefix_sep='').groupby(level=0, axis=1).max()
    
    # Perform FP-Growth to find frequent itemsets
    frequent_itemsets = fpgrowth(encoded_titles, min_support=0.1, use_colnames=True)

    return frequent_itemsets

def consume_data():
    consumer = KafkaConsumer(
        'product_info_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='fp-growth-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    titles = []
    try:
        for message in consumer:
            item = message.value
            print(f"Received data: {item}")  # Debug to check data receipt
            title = item.get('title', '')  # Handle missing or None titles
            tokenized_title = tokenize_title(title)
            titles.append(tokenized_title)

            if len(titles) >= 10:  # Process in batches of 10 titles
                frequent_itemsets = mine_frequent_itemsets(titles)
                print("Frequent Itemsets:\n", frequent_itemsets)

                titles = []  # Clear the batch after processing
    except KeyboardInterrupt:
        print("Stopped consuming")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_data()

