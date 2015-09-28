from bson.code import Code
from bson.son import SON
import json
import config
import pymongo
import time
import sys
import re

#remove these until wordcloud is important
# import matplotlib.pyplot as plt
# from wordcloud import WordCloud

from pymongo import MongoClient

def convertTimestamp_msTo5MinIntervalString(s_timestamp):
    return time.strftime("%m%d|%H:%M",time.gmtime((((int(s_timestamp)+150000)/300000)*300000)/1000))


def addToDictionary(dictionary, word):
    if (word.upper() in littlewords):
      if config.debug:
          print "GOTCHA in littlewords ({0}), we don't want you in here".format(word)
      return

    if (word.upper() in littleverbs):
      if config.debug:
          print "GOTCHA in litteverbs ({0}), we don't want you in here".format(word)
      return

    if (word.upper() in starters):
      if config.debug:
          print "GOTCHA in starters ({0}), we don't want you in here".format(word)
      return

    if len(word) < 3:
      return

    if word.lower() in dictionary.keys():
      if config.debug:
        print "It's in here so increment"
      dictionary[word.lower()] += 1
    else:
      if config.debug:
        print "Not here so adding"
      dictionary[word.lower()] = 1
       
def loadDictionaryToMongo(dictionary, grouper):
    count = 0
    for item in dictionary:
      try:
        db[config.s_WorkingResults].insert_one({"t_grouper": grouper, "word": item, "count": dictionary[item]})
        count += 1
      except:
        print "Failed to insert"    
    return count
 
dict_WordCount = {}    
s_tg_last = 0
counter = 0

littlewords = ['AND', 'THE', 'FOR', 'YOU', 'ALL', 'NOT', 'NO', 'YOURS', 'OR', 'SO', 'FOR', 'ME', 'HE', 'HIM', 'HIS', 'IN', 'IF', 'WITH', 'OF', 'WHAT', 'WHO', 'WHY', 'HOW', 'WHEN', 'WHERE', 'AS', 'AN', 'AT', 'THE', 'IT', 'FROM', 'TO', 'BY', 'BUT', 'ONLY', 'YOU', 'YOUR', 'WE', 'WILL', 'WOULD', 'THAT', 'THEY', 'THIS', 'ON', 'MY'];
littleverbs = ['DO', 'BE', 'IS', 'WAS', 'ARE', 'WERE', 'HAS', 'HAD', 'HAVE'];
starters = ['HTTP', 'HTTPS' ]

client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
db = client[config.s_WorkingDatabase]

print "Retrieving records"
results = db[config.s_WorkingCollection].find({}, {"id":1, "timestamp_ms":1}).sort("timestamp_ms",pymongo.ASCENDING)
print "Updating records"
for item in results:
    retval = db[config.s_WorkingCollection].update_one({"id": item['id']}, { "$set": { "t_grouper": convertTimestamp_msTo5MinIntervalString(item['timestamp_ms'])}})
    counter += 1

print "{0} records updated with t_grouper".format(counter)
print "-"*50
print "Retrieving records with t_grouper tag for mapping"
results = db[config.s_WorkingCollection].find({}, {"t_grouper":1, "id":1, "text":1,"timestamp_ms":1,"created_at":1,"lang":1,"in_reply_status_id":1,"in_reply_to_screen_name":1,"entities.hastags":1,"user_mentions":1,"retweeted":1}).sort("t_grouper",pymongo.ASCENDING)
p = re.compile(r'\W+')

print "Mapping tweet text to word count"
for item in results:
    s_tg_this = item['t_grouper']
    if s_tg_this != s_tg_last:
        # write out the last one with the t_grouper and start again
        if s_tg_last != 0:
            print "writing for {0} t_grouper".format(s_tg_last)
            print loadDictionaryToMongo(dict_WordCount, s_tg_last)
        #start a new aggregator
        # this dictionary will hold all the words encountered and a count of the times found
        dict_WordCount = {}

    #split the text
    
    for word in p.split(item['text'].lower()):
        addToDictionary(dict_WordCount, word)
        
    #assign this t_group to being the last
    s_tg_last = s_tg_this
    

# for item in dict_WordCount:
print "final dict_count is {0}".format(len(dict_WordCount))
print "-"*50