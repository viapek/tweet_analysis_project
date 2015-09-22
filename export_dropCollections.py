import config
import subprocess
import time
import os
from pymongo import MongoClient

#initiate counters at 0
i_DropCounter = 0
i_ExportCounter = 0
while True:
    d_OfflineToHere = raw_input("Please enter a date (MMDD). All Collections up to AND including this date will be effected. GMT[{0}]".format(time.strftime('%m%d', time.gmtime())))

    if d_OfflineToHere == '':
        d_OfflineToHere = time.strftime('%m%d', time.gmtime())
    if len(d_OfflineToHere) != 4:
        print "Please enter a date in MMDD format, i.e. {0}".format(time.strftime('%m%d', time.gmtime()))
        continue

    s_ExportDropOrBoth = raw_input("Do you wish to [E]xport, [D]rop, or [B]oth:")
    if s_ExportDropOrBoth in ['B','D','E']:
        if s_ExportDropOrBoth == 'B':
            b_Export = True
            b_Drop = True
            s_Confirmation = "exported and dropped"
        else:
            if s_ExportDropOrBoth == 'D':
                b_Export = False
                b_Drop = True
                s_Confirmation = "dropped"
            else:
                b_Export = True
                b_Drop = False
                s_Confirmation = "exported"
    else:
        continue


    s_Confirm = raw_input("Collections up and including {0} will be {1}. Is that correct [Y/N]".format(d_OfflineToHere, s_Confirmation))
    if s_Confirm == "Y":
        break
    else:
        continue

#setup an array for tracking actions
ary_PlannedActions = []
# get a hook on the database
client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
m_Dbase = client[config.s_Dbase]
#get a list of all the collections without systyem collections
tw_Collections = m_Dbase.collection_names(False)

if config.debug:
    print "Start iterating through tw_Collections with {0} items".format(len(tw_Collections))
for collection in tw_Collections:
    if config.debug:
        print "Working with {0}".format(collection)
        print "-"*50
    if not collection.endswith(config.s_colSuffix): #if it ain't matching our collection suffix, skip it
        ary_PlannedActions.append("Don't care about {0} because it doesn't match suffix {1}".format(collection,config.s_colSuffix))
        if config.debug:
            print "- suffix not {0}".format(config.s_colSuffix)
        continue
        
    if int(collection[0:4]) <= int(d_OfflineToHere):
        if config.debug:
            print "|  Got a handle on {0}".format(collection)
        
        if b_Export and b_Drop:
            b_DropOK = False
        else:
            b_DropOK = True
         
        if b_Export:
            s_ExportCommand = "mongodump -u {0} -p {1} -d {2} -c {3} -o {4}".format(
                    config.s_MongoUser,config.s_MongoPassword,config.s_Dbase, collection, config.s_OfflineCollectionPath)
            print s_ExportCommand
            proc = subprocess.Popen(s_ExportCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                
            if proc.wait() == 0:
                b_DropOK = True
            else:
                b_DropOK = False
                ary_PlannedActions.append("Failed to export {0}".format(collection))
                
            for line in proc.stdout.readlines():
                #get the file name and document count
                if line.find("document") > 1:
                    if config.debug:
                        print "Here's our document count: {0}".format(line.split()[1])
                        
                if [s for s in line.split() if s.endswith('bson')]:
                    i_FileSizeBefore = os.stat(s).st_size/1024
                  
                    s_GZipCommand = "gzip -9 {0}".format(s)
                    print s_GZipCommand
                    
                    
                    if config.debug:
                        print "File {0} exported. File size was {1}kB".format(s,i_FileSizeBefore)    
        
            ary_PlannedActions.append("Exported {0} to {1}. Size before compression: {2}kB".format(collection,config.s_OfflineCollectionPath+"/"+config.s_Dbase,i_FileSizeBefore))
            i_ExportCounter += 1
            subprocess.Popen(s_GZipCommand, shell=True)
            
        if b_DropOK and b_Drop:
            try:
                m_Dbase.drop_collection(collection)
            except (ValueError, KeyError, TypeError) as e:
                print e
            ary_PlannedActions.append("Dropped {0} collection successfully".format(collection))
            i_DropCounter += 1
        else:
            if not b_DropOK:
                ary_PlannedActions.append("Did not drop {0} as export failed".format(collection))

print ary_PlannedActions,"\n"
print "{0} collections evaluated. \n".format(len(tw_Collections))
if b_Drop:
    print "- {0} successfully dropped.".format(i_DropCounter)
if b_Export:
    print "- {0} successfully exported.".format(i_ExportCounter)
print "\n"