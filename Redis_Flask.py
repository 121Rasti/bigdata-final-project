# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import threading
import time
from flask import Flask  # From module flask import class Flask
app = Flask(__name__)    # Construct an instance of Flask class for our webapp #ffe6e6lightgrey

import redis
from datetime import datetime, timedelta
import json 

def find_date(time):
    return time[:4] + '-' + time[5:7] + '-' + time[8:10] + '-' + time[11:13]
def find_date_from_obj(date):
    return str(date.year) +'-'+ dup_str(date.month) +'-'+ dup_str(date.day) +'-'+ dup_str(date.hour)
def dup_str(x):
    return '0' + str(x) if len(str(x)) == 1 else str(x)
def find_expiration_minutes(expDay):
    return expDay * 24 * 3600

redis = redis.Redis(host='localhost', port=6379)
Q1 = ""
Q2 = ""
Q3 = ""
Q4 = ""
Q5 = ""
hashtagsList = []
tweetsList = []
value = 0
def proccess():
    global Q1
    global Q2 
    global Q3
    global Q4
    global Q5
    global hashtagsList
    global tweetsList
    while True:
        time.sleep(0.01)
        f2 = open('preprocessed_data.json', encoding="utf8")
        data = json.load(f2)
        f2.close()
        
        expirationMinutes = find_expiration_minutes(7)
        
        for item in data:
            key = "username: " + item["senderUsername"] + " in: " + find_date(item["sendTime"])
            if redis.exists(key) == 0:
                redis.set(key, 1, ex=expirationMinutes)
            else:
                redis.incr(key)
                
            key = "tweets: " + find_date(item["sendTime"])
            if redis.exists(key) == 0:
                redis.set(key, 1, ex=expirationMinutes)
            else:
                redis.incr(key)
                
            if "" not in item["_meta"]["hashtags"] and len(item["_meta"]["hashtags"]) != 0:
                key = "unique hashtags: " + find_date(item["sendTime"])
                if redis.exists(key) == 0:
                    redis.sadd(key, *item["_meta"]["hashtags"])
                    redis.expire(key, expirationMinutes)
                else:
                    redis.sadd(key, *item["_meta"]["hashtags"])
                    
                if redis.exists("hashtags_list") == 0:
                    redis.lpush("hashtags_list", *item["_meta"]["hashtags"])
                    redis.expire("hashtags_list", expirationMinutes)
                else:
                    redis.lpush("hashtags_list", *item["_meta"]["hashtags"])
                    if redis.llen("hashtags_list") > 1000: 
                        redis.ltrim("hashtags_list", 0, 999)  
            
        if redis.exists("tweets_list") == 0:
            redis.lpush("tweets_list", item["content"])
            redis.expire("tweets_list", expirationMinutes)
        else:
            redis.lpush("tweets_list", item["content"])
            if redis.llen("tweets_list") > 100:
                redis.ltrim("tweets_list", 0, 99)
                
        
        lastHours = 6
        now = datetime(2021, 7, 19, 18)
        
        totalTweets = 0
        for h in range(lastHours + 1):
            goalDate = now - timedelta(hours=h)
            key = "tweets: " + find_date_from_obj(goalDate)
            if redis.exists(key) > 0:
                totalTweets += int(redis.get(key).decode("utf-8"))
        
        Q1 = "در " + str(lastHours) +" ساعت اخیر " + str(totalTweets) + " توییت دریافت شده است."
        
        dateFrom = datetime(2021, 7, 1, 18)
        dateTo = datetime(2021, 7, 19, 18)
        
        delta = timedelta(hours=1)
        i = dateFrom
        totalTweets = 0
        while i <= dateTo:
            key = "tweets: " + find_date_from_obj(i)
            if redis.exists(key) > 0:
                totalTweets += int(redis.get(key).decode("utf-8"))
            i += delta
        
        Q2 = "از تاریخ " + str(dateFrom) +" تا " + str(dateTo) +" تعداد " + str(totalTweets) + " توییت دریافت شده است."
            
        lastHours = 1
        totalHashtags = 0
            
        for h in range(lastHours + 1):
            goalDate = now - timedelta(hours=h)
            key = "unique hashtags: " + find_date_from_obj(goalDate)
            if redis.exists(key) > 0:
                totalHashtags += redis.scard(key)
        
        Q3 = "در " + str(lastHours) +" ساعت اخیر " + str(totalHashtags) +  " هشتگ زده شده است."
        
        
        indexFrom = 0
        indexTo = -1
        
        #hashtagsList = []
        for item in redis.lrange("hashtags_list", indexFrom, indexTo):
            hashtagsList.append(item.decode("utf-8"))
        
        Q4 =  str(hashtagsList)     
        
        
        
        indexFrom = 0
        indexTo = -1
        #tweetsList = []
        #print(tweetsList)
        for item in redis.lrange("tweets_list", indexFrom, indexTo):
            tweetsList.append(item.decode("utf-8"))
        
        Q5 = "صد توییت آخر این ها هستند: "
        
    

@app.route('/')    
@app.route('/home') 

def home():
    s = "<html><head><style>p.ex1 { border: 1px solid green;padding: 20px;margin: 20px;}</style></head><body>"
    s = s + "<h2>" + Q1 +  "</h2>"
    s = s + "<h2>" + Q2 +  "</h2>"
    s = s + "<h2>" + Q3 +  "</h2>"
    s = s + "<h2>" +  "هزار هشتگ آخر این ها هستند: " +  "</h2>"
    s = s + "<p  class='ex1', style='background-color: #ffffff'>" + Q4 +  "</p>"
    s = s + "<h1>" + Q5 +  "</h1>"
    for x in tweetsList:
        s = s + "<p  class='ex1', style='background-color: #ffffff'>" + str(x) +  "</p>"    
    s = s + "</tr>"
    return "<html dir='rtl' lang='ar'><body style='background-color: lightgrey'>" + s + "</body></html>" 


if __name__ == '__main__':  # Script executed directly?
    proccess_thread = threading.Thread(target=proccess)
    proccess_thread.setDaemon(True)
    proccess_thread.start()
    app.run(debug = True)  # Launch built-in web server and run this Flask webapp
    