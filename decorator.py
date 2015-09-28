from bson.code import Code
from bson.son import SON
import json
import config
import pymongo
import time

from pymongo import MongoClient
# connect to dbase from config file
client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
db = client[config.s_WorkingDatabase]
# connect to the intermediary store, i.e. the map reduced collection
colHandle = db[config.s_WorkingResults]
#set the pipe to aggreagete on the time interval and then list the word and count during each window
pipe = [{"$group": { "_id": { "t_grouper": "$t_grouper", "word": "$word"}, "wordCount": { "$sum" : "$count" } } }, { "$group": { "_id": "$_id.t_grouper", "words" : { "$push": { "word": "$_id.word", "count": "$wordCount"},},"count": { "$sum": "$wordCount"} }}, { "$sort": {"_id":1}}, {"$limit":10}]
#run it
result = colHandle.aggregate(pipeline=pipe)
#cycle through line. Each line represents a time interval
for line in result:
    #set the significant word counter, i.e. the top how many words per interval do we care about
    x = 0
    #do what ever formatting stuff you want to do
    print "Word count for interval {0}".format(line['_id'])
    print "Total words counted {0}".format(line['count'])
    print "-"*50
    # now iterate through the list of words and their count sorted by count (this is loose as it expects the count to come first.
    #TODO specify 'count' as the sort key
    for item in sorted(line['words'], reverse=True):
        if x == 20:
            #if we are done with the top x, brebak out to the next line
            break
        else:
            # print and increment
            print "{0}: {1}".format(item['word'],item['count'])
            x += 1
        
    print "-"*50
    