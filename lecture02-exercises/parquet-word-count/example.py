from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice-in-wonderland.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)

    # create parquet file with wordcount data
    df = pd.DataFrame(wordcount, columns=['word', 'count'])

    df.to_parquet('wordcount.parquet', engine='pyarrow', compression='snappy')
    client.upload('/wordcount.parquet', 'wordcount.parquet', overwrite=True)
