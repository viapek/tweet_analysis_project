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
           "   var t_time = parseInt(this.timestamp_ms);"
           "   t_time = (t_time+150000)/300000;"
           "   t_time = parseInt(t_time)*300;"
           "   var cnt = this.text;"
           "   if (!cnt || isNaN(t_time)) {"
           "        return;"
           "   }"
           "   var words = cnt.match(/\w+/g);"
           "   if (words == null) {"
           "      return;"
           "   }"
           "   var elements = {};"
           "   for (var i = 0; i < words.length; i++) {"
           "     if (words[i].toUpperCase() in littlewords || words[i].toUpperCase() in littleverbs) {"
           "     } else {"
           "        if (words[i].toLowerCase() in elements) {"
           "            elements[words[i].toLowerCase()]++;"
           "        } else {"
           "            elements[words[i].toLowerCase()] = 1;"
           "        }"
           "      }"
           "    }"
           "    emit({ 'id': t_time }, {'words': elements});"
           "}")


reduce = Code("function reduce(key, values) { "
            "    var elements = {};"
            "    for (var i = 0; i < values.length; i++) {"
            "        for (var words in values[i]) {"
            "            for (var word in values[i].words){ "
            "                if (word in elements){"
            "                    elements[word] = elements[word] + values[i].words[word];"
            "                } else {"
            "                    elements[word] = values[i].words[word];"
            "                }"
            "            }"
            "        }"
            "    }"
            "    return elements;"
            "}")

colHandle = db[config.s_WorkingCollection]

colHandle.map_reduce(map, reduce, config.s_WorkingResults)
