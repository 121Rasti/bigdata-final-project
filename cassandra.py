from calendar import month
from itertools import count
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

from cassandra.util import Date

KEYSPACE = "final_project"
def drop_tabels():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    
    session.execute("""
        DROP TABLE IF EXISTS fullData ;
        """)
    session.execute("""
        DROP TABLE IF EXISTS hashtag ;
        """)
    session.execute("""
        DROP TABLE IF EXISTS keyWord ;
        """)
    session.shutdown()

def insert(data):
    for i in data:
        insert_data(i)

def insert_data(data):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
        """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS fullData ( 
            canal text,
            sendTime timestamp,
            id text , 
            senderName text , 
            content text , 
            senderUsername text ,  
            PRIMARY KEY (canal, sendTime, id) );
        """)

    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS canal ( 
    #         title_album text , 
    #         track_id int , 
    #         title_track text , 
    #         PRIMARY KEY (title_album, title_track) );
    #     """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS  hashtag ( 
            canal text,
            hashtag text,
            sendTime timestamp,
            id text , 
            senderName text , 
            content text , 
            senderUsername text ,  
            PRIMARY KEY (canal, hashtag, sendTime, id) );
        """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS  keyWord ( 
            canal text,
            keyword text,
            sendTime timestamp,
            id text , 
            senderName text , 
            content text , 
            senderUsername text ,  
            PRIMARY KEY (canal, keyword, sendTime, id) );
        """)

    insert_fullData = session.prepare("""
            INSERT INTO fullData (  
                                canal,
                                sendTime,
                                id,
                                senderName,
                                content,
                                senderUsername
                                )
            VALUES (?, ?, ?, ?, ?, ?)
            IF NOT EXISTS; 
            """)
    insert_hashtag = session.prepare("""
            INSERT INTO hashtag (  
                                canal,
                                hashtag,
                                sendTime,
                                id,
                                senderName,
                                content,
                                senderUsername
                                )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            IF NOT EXISTS; 
            """)
    insert_keyWord = session.prepare("""
            INSERT INTO keyWord (  
                                canal,
                                keyword,
                                sendTime,
                                id,
                                senderName,
                                content,
                                senderUsername
                                )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            IF NOT EXISTS; 
            """)
    canal = 'sahamyab'
    id = data['id']
    sendTime = datetime.strptime(data['sendTime'], '%Y-%m-%dT%H:%M:%SZ')
    date_released= data['sendTime'].split('T')
    date_miladi=date_released[0]
    sendTimePersian=data['sendTimePersian']
    date_released= sendTimePersian.split(' ')
    YearPersian=date_released[0].split("/")[0]
    MonthPersian=date_released[0].split("/")[1]
    DayPersian=date_released[0].split("/")[2]
    hour = int(date_released[1].split(":")[0])

    senderName=data['senderName']
    senderUsername=data['senderUsername']
    content=data['content']
    type=data['type']
    _meta=data['_meta']
    hashtags = _meta['hashtags']
    key_words = _meta['key_words']
    links=data['links']
    session.execute(insert_fullData, [canal, sendTime,id,senderName,content,senderUsername])
    for hash in hashtags:
        session.execute(insert_hashtag, [canal, hash, sendTime ,id,senderName,content,senderUsername])
    for keyword in key_words:
        session.execute(insert_keyWord, [canal, keyword, sendTime,id,senderName,content,senderUsername])

    #closing Cassandra connection
    session.shutdown()

def all_post(timestamp1 , timestamp2, canal = 'sahamyab' ):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    code = session.prepare("""
            SELECT * FROM fullData  
            WHERE canal = ? AND sendTime >= ? AND sendTime <= ?;
            """)
    result = session.execute(code, [canal, timestamp1, timestamp2])
    return result

def all_post_by_hashtag(hashtag, timestamp1 , timestamp2, canal = 'sahamyab'):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    code = session.prepare("""
            SELECT * FROM hashtag  
            WHERE canal = ? AND hashtag = ? AND sendTime >= ? AND sendTime <= ?;
            """)
    result = session.execute(code, [canal, hashtag, timestamp1, timestamp2])
    return result

def all_post_by_keyWord(keyword, timestamp1 , timestamp2, canal = 'sahamyab' ):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    code = session.prepare("""
            SELECT * FROM keyWord  
            WHERE canal = ? AND keyWord = ? AND sendTime >= ? AND sendTime <= ?;
            """)
    result = session.execute(code, [canal,keyword,timestamp1, timestamp2])
    return result

def get_post(n):
    timestamp2 = datetime.today()
    timestamp1 = timestamp2 - timedelta(hours=n)
    n_hour = all_post(timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(days=n)
    n_day = all_post(timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(weeks=n)
    n_week = all_post(timestamp1 , timestamp2)

    m = timestamp2.month - int(n % 12)
    y = timestamp2.year - int(n / 12)
    if m <= 0:
        m = 12 + m
        y -= 1
    timestamp1 = datetime.strptime(f'{y}-{m}-{timestamp2.day}', '%Y-%m-%d')
    n_month = all_post(timestamp1 , timestamp2)

    timestamp1 = datetime.strptime(f'{timestamp2.year-n}-{timestamp2.month}-{timestamp2.day}', '%Y-%m-%d')
    n_year = all_post(timestamp1 , timestamp2)
    
    return n_hour, n_day, n_week, n_month, n_year

def get_post_by_hashtag(n, hashtag):
    timestamp2 = datetime.today()
    timestamp1 = timestamp2 - timedelta(hours=n)
    n_hour = all_post_by_hashtag(hashtag, timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(days=n)
    n_day = all_post_by_hashtag(hashtag, timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(weeks=n)
    n_week = all_post_by_hashtag(hashtag, timestamp1 , timestamp2)

    m = timestamp2.month - int(n % 12)
    y = timestamp2.year - int(n / 12)
    if m <= 0:
        m = 12 + m
        y -= 1
    timestamp1 = datetime.strptime(f'{y}-{m}-{timestamp2.day}', '%Y-%m-%d')
    n_month = all_post_by_hashtag(hashtag, timestamp1 , timestamp2)

    timestamp1 = datetime.strptime(f'{timestamp2.year-n}-{timestamp2.month}-{timestamp2.day}', '%Y-%m-%d')
    n_year = all_post_by_hashtag(hashtag, timestamp1 , timestamp2)
    
    return n_hour, n_day, n_week, n_month, n_year
    
def get_post_by_keyWord(n, keyword):
    timestamp2 = datetime.today()
    timestamp1 = timestamp2 - timedelta(hours=n)
    n_hour = all_post_by_keyWord(keyword, timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(days=n)
    n_day = all_post_by_keyWord(keyword, timestamp1 , timestamp2)

    timestamp1 = timestamp2 - timedelta(weeks=n)
    n_week = all_post_by_keyWord(keyword, timestamp1 , timestamp2)

    m = timestamp2.month - int(n % 12)
    y = timestamp2.year - int(n / 12)
    if m <= 0:
        m = 12 + m
        y -= 1
    timestamp1 = datetime.strptime(f'{y}-{m}-{timestamp2.day}', '%Y-%m-%d')
    n_month = all_post_by_keyWord(keyword, timestamp1 , timestamp2)

    timestamp1 = datetime.strptime(f'{timestamp2.year-n}-{timestamp2.month}-{timestamp2.day}', '%Y-%m-%d')
    n_year = all_post_by_keyWord(keyword, timestamp1 , timestamp2)
    
    return n_hour, n_day, n_week, n_month, n_year


import json 
# use the below code to read the data
f2 = open('preprocessed_data.json', encoding="utf8")
jdata = json.load(f2)
f2.close()

# insert(jdata)
a_p = get_post(3)
h = get_post_by_hashtag(3, 'شپلی')
k = get_post_by_keyWord(3, '‫اقتصاد‬')
