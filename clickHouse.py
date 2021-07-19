import json
from clickhouse_driver import Client
from datetime import datetime

client = Client('localhost')
client.execute('DROP TABLE IF EXISTS default.sahamyab')
client.execute("""CREATE TABLE IF NOT EXISTS default.sahamyab 
               (
               id String,
               sendTime DateTime,
               sendTimePersian String,
               hashtags Array(Nullable(String)),
               key_words Array(Nullable(String)),
               senderUsername String,
               senderName String,
               content String
               )
                ENGINE = MergeTree PARTITION BY toYYYYMMDD(sendTime) ORDER BY toYYYYMMDD(sendTime)
               """)


def insert(jData):
    data = [[
        jData['id'],
        datetime.strptime(jData['sendTime'], '%Y-%m-%dT%H:%M:%SZ'),
        jData['sendTimePersian'],
        jData['_meta']['hashtags'],
        jData['_meta']['key_words'],
        jData['senderUsername'],
        jData['senderName'],
        jData['content']
    ]]
    result = client.execute('INSERT INTO default.sahamyab '
                            '('
                            'id,'
                            'sendTime,'
                            'sendTimePersian,'
                            'hashtags,'
                            'key_words,'
                            'senderUsername,'
                            'senderName,'
                            'content'
                            ')'
                            ' VALUES ', data)
    print(jData['id'])
    return True

def insert_jsons(data):
    for i in data:
        insert(i)

import json 
# use the below code to read the data
f2 = open('preprocessed_data.json', encoding="utf8")
jdata = json.load(f2)
f2.close()
insert_jsons(jdata)