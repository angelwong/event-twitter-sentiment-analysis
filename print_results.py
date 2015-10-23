import os, sys, string, time, re
import requests, json, urllib, urllib2, base64
import pymongo
from multiprocessing import Pool, Lock, Queue, Manager
import collections
import subprocess

def print_results():
    
    db = pymongo.MongoClient().twitter_db
    tweets = db.tweets_toy

    neg_tweets = tweets.find({"sentiment" : {"$lt" : 0}})

    most_neg_tweets = tweets.aggregate([
        {"$unwind" : "$keyword"},
        {"$match": {"sentiment": {"$lt":0}}},
        {"$group": {"_id": "$keyword", "count": {"$sum":1}, "avgScore": {"$avg": "$sentiment"}, "tweets":{"$push": {"content": "$text", "sentiment": "$sentiment"}}}},
        {"$sort": {"count": -1, "avgScore":-1, "sentiment": -1}},
        {"$match": {"count": {"$gt":3}}}, #threshold = 4 now
        {"$out": "most_neg_tweets"}
        ])

    subprocess.call("mongoexport --db twitter_db --collection most_neg_tweets --fields '_id', 'tweets' --out " + "event_entities.json", shell=True)

    return

if __name__ == "__main__":

    print_results()