from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(deque)


for message in consumer:
    event = message.value
    user_id = event['user_id']

    tx_time = datetime.fromisoformat(event['timestamp'])
    tx_queue = user_transactions[user_id]
    tx_queue.append(tx_time)
    
    limit_time = tx_time - timedelta(seconds=60)
    while tx_queue[0] < limit_time:
        tx_queue.popleft()

    if len(tx_queue) > 3:
        print(f"ALERT: user {user_id} wykonał {len(tx_queue)} transakcji w ciągu 60 sekund.\n")
