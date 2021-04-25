from kafka import KafkaProducer
import umsgpack
import pandas as pd
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=umsgpack.dumps)

df = pd.read_csv('data.csv', encoding='latin', header=None)

for index, row in df.iterrows():
    date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    print(row[1], date, row[4], row[5])
    producer.send('tweets', {'id': row[1], 'date': date, 'user': row[4], 'text': row[5]})
