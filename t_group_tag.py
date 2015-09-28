from bson.code import Code
from bson.son import SON
import json
import config
import pymongo
import time

from pymongo import MongoClient

def convertTimestamp_msTo5MinIntervalString(s_timestamp):
    return time.strftime("%m%d|%H:%M",time.gmtime((((int(s_timestamp)+150000)/300000)*300000)/1000))

counter = 0
client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
db = client[config.s_WorkingDatabase]

results = db[config.s_WorkingCollection].find({}, {"id":1, "text":1,"timestamp_ms":1,"created_at":1,"lang":1,"in_reply_status_id":1,"in_reply_to_screen_name":1,"entities.hastags":1,"user_mentions":1,"retweeted":1}).sort("timestamp_ms",pymongo.ASCENDING)

for item in results:
    retval = db[config.s_WorkingCollection].update_one({"id": item['id']}, { "$set": { "t_grouper": convertTimestamp_msTo5MinIntervalString(item['timestamp_ms'])}})
    counter += 1
    
print "{0} records updated with t_grouper".format(counter)