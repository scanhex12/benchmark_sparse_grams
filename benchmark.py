from datasets import load_dataset
from clickhouse_driver import Client
import pandas as pd
from tqdm.auto import trange
import time

dataset = load_dataset('ag_news')

client = Client(host='localhost')

client.execute("DROP TABLE IF EXISTS text_data2")
client.execute("DROP TABLE IF EXISTS text_data")
client.execute('''
CREATE TABLE IF NOT EXISTS text_data2 (
    id UInt32,
    text String,
    INDEX bf (text) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
''')
client.execute('''
CREATE TABLE IF NOT EXISTS text_data (
    id UInt32,
    text String,
    INDEX bf (text) TYPE sparse_gram(5, 1000000, 512, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
''')

data = {
    'id': [],
    'text': []
}
for i in trange(100000):
    data['id'].append(i)
    data['text'].append(dataset['train'][i]['text'])

df = pd.DataFrame(data)

data_to_insert = list(df.to_records(index=False))

client.execute('INSERT INTO text_data2 (id, text) VALUES', data_to_insert)
client.execute('INSERT INTO text_data (id, text) VALUES', data_to_insert)

strs = []
for i in trange(100000):
    for word in dataset['train'][i]['text'].split():
        if '\\' not in word and '%' not in word and "'" not in word:
            strs.append(word)

queries = sorted(strs, key=len, reverse=True)[:15]

start = time.time()
for query in queries:
    client.execute(f"SELECT * FROM text_data2 WHERE text LIKE '%{query}%'")
end = time.time()

print(f"OK ngram: {end - start}")

start = time.time()
for query in queries:
    client.execute(f"SELECT * FROM text_data WHERE text LIKE '%{query}%'")
end = time.time()

print(f"OK sparse: {end - start}")
