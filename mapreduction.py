from bson.code import Code
from bson.son import SON
import json
import config
import pymongo
#remove these until wordcloud is important
# import matplotlib.pyplot as plt
# from wordcloud import WordCloud

from pymongo import MongoClient

client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
db = client[config.s_WorkingDatabase]

map = Code("function map() {"
           "   var littlewords = ['ALL', 'NOT', 'NO', 'YOURS', 'OR', 'SO', 'FOR', 'ME', 'HE', 'HIM', 'HIS', 'IN', 'IF', 'WITH', 'OF', 'WHAT', 'WHO', 'WHY', 'HOW', 'WHEN', 'WHERE', 'AS', 'AN', 'AT', 'THE', 'IT', 'FROM', 'TO', 'BY', 'BUT', 'ONLY', 'YOU', 'YOUR', 'WE', 'WILL', 'WOULD', 'THAT', 'THEY', 'THIS', 'ON', 'MY'];"
           "   var littleverbs = ['DO', 'BE', 'IS', 'WAS', 'ARE', 'WERE', 'HAS', 'HAD', 'HAVE'];"
           "   var cnt = this.text;"
           "   var words = cnt.match(/\w+/g);"
           "   if (words == null) {"
           "        return;"
           "   }"
           "   for (var i = 0; i < words.length; i++) {"
           "        if (words[i].length > 1) {"
           "            if ((littlewords.indexOf(words[i].toUpperCase()) ==  -1) && (littleverbs.indexOf(words[i].toUpperCase()) ==  -1)) {"
           "                emit({word:words[i].toLowerCase() }, {count:1 });"
           "            }"
           "        }"
           "   }"
           "}")

reduce = Code("function reduce(key, counts) {"
            "   var cnt = 0;"
            "   for (var i = 0; i < counts.length; i++) {"
            "      cnt = cnt + counts[i].count;"
            "   }"
            "   return { count:cnt };"
            "}")

colHandle = db[config.s_WorkingCollection]

result = colHandle.map_reduce(map, reduce, config.s_WorkingResults)

try:
  db.drop_collection(colHandle.name)
except (ValueError, KeyError, TypeError) as e:
  print "Error in dropping working collection:"
  print e
else:
  print "Dropped like it was hot"

colHandle = db[config.s_WorkingResults]

pipe = [{'$match': { 'value.count': { '$gte': 1} }}, { '$sort': { 'value.count': -1}}, {'$limit':50}]
result = colHandle.aggregate(pipeline=pipe)

s_ForWordCloud = ''

for line in result:
    print line
    #s_ForWordCloud += "{0} ".format(line.values()[0]['word'])*int(line.values()[1]['count'])
"""     
wordcloud = WordCloud().generate(s_ForWordCloud)
img = plt.imshow(wordcloud)
plt.axis("off")
plt.show()
"""