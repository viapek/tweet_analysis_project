import config
import subprocess
import time
import os
from pymongo import MongoClient

##initiate counters at 0
i_DropCounter = 0
i_ExportCounter = 0
i_TotalDataSize = 0
i_TotalCollectionDataSize = 0

i_FileSizeBefore = 0

def getUserInput():
  global d_OfflineToHere
  global s_ExportDropOrBoth
  global b_Export
  global b_Drop

  s_OfflineToHereInputMsg = "Please enter a date (MMDD). All Collections up to AND including this date will be effected. GMT[{0}]".format(time.strftime('%m%d', time.gmtime()))
  while True:
    d_OfflineToHere = raw_input(s_OfflineToHereInputMsg)

    if d_OfflineToHere == '':
      d_OfflineToHere = time.strftime('%m%d', time.gmtime())
    if len(d_OfflineToHere) != 4:
      s_OfflineToHereInputMsg = "Please enter a date in MMDD format, i.e. {0}".format(time.strftime('%m%d', time.gmtime()))
      continue
    else:
      break
    
  while True:
    s_ExportDropOrBoth = raw_input("Do you wish to [E]xport, [D]rop, [B]oth or e[X]it:")
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
      break
    else:
      if s_ExportDropOrBoth == 'X':
          return False
      continue

  s_Confirm = raw_input("Collections up and including {0} will be {1}. Is that correct [Y/N]".format(d_OfflineToHere, s_Confirmation))
  if s_Confirm == "Y":
    return True
  else:
    return False

if __name__ == '__main__':

  client = MongoClient(config.s_MongoDBHost, config.i_MongoDBPort)
  m_Dbase = client[config.s_Dbase]
    #get a list of all the collections without systyem collections
  tw_Collections = sorted(m_Dbase.collection_names(False))

  s_prevPreFix = []
  for f in tw_Collections:  
    if not f[0:4] in s_prevPreFix:
      s_prevPreFix.append(f[0:4])
          
  print "The range of possible dates: "
  for s_range in s_prevPreFix:
      print "{0}".format(s_range)    

  if getUserInput():
    #setup an array for tracking actions
    ary_PlannedActions = []

    if config.debug: 
      print "Start iterating through tw_Collections with {0} items".format(len(tw_Collections))
    
    for collection in tw_Collections:

      if config.debug:
        print "Working with {0}".format(collection)
        print "-"*50
        
      #if it ain't matching our collection suffix, skip it
      if not collection.endswith((config.s_colSuffix,config.s_StitchSuffix)): 
        ary_PlannedActions.append("Don't care about {0} because it doesn't match suffix {1}".format(collection,config.s_colSuffix))

        if config.debug:
          print "- suffix not {0}".format(config.s_colSuffix)
          
        # continue to the next list item
        continue
      
      #test to see if we can convert the collection prefic to an int
      try:
        int(collection[0:4])
      except:
        #if not continue to the next list item
        continue

      #check if the colleciton prefix is earlier than the value provided by the user      
      if int(collection[0:4]) <= int(d_OfflineToHere):
        
        
        if config.debug:
          print "|  Got a handle on {0}".format(collection)
        
        # if we are to perform export and drop we don't want to allow drop before successful export
        if b_Export and b_Drop:
          b_DropOK = False
        else:
          #if we are only dropping then trust that they want it dropped
          b_DropOK = True
         
        if b_Export:
          #build the mongodump command string
          s_ExportCommand = "mongodump -u {0} -p {1} -d {2} -c {3} -o {4}".format(
                    config.s_MongoUser,
                    config.s_MongoPassword,
                    config.s_Dbase, 
                    collection, 
                    config.s_OfflineCollectionPath)
          
          if config.debug:
            print s_ExportCommand
          
          #excute the command and get feedback
          proc = subprocess.Popen(s_ExportCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

          if proc.wait() == 0:
            #if the return code is 0 we can go and drop the table
            b_DropOK = True
          else:
            #otherwise log it and do not drop the table
            b_DropOK = False
            ary_PlannedActions.append("Failed to export {0}".format(collection))

          #read the output and extract the useful data
          for line in proc.stdout.readlines():
            
            #if config.debug:
            #   print line
                
            #get the file name and document count
            if line.find("document") > 1:
              if config.debug:
                print "Here's our document count: {0}".format(line.split()[1])
              #if we find it, go to the next item
              continue
          
            #s = line.split()
            #if 
            #split the line and look for the .bson ending in the strings
            if [s for s in line.split() if s.endswith('.bson')]:
              #use the file name to get the filesize
              i_FileSizeBefore = os.stat(s).st_size/1024
              #add it to our total counter
              i_TotalDataSize += i_FileSizeBefore
              #create our gzip string
              s_GZipCommand = "gzip -9 {0}".format(s)

              try:
                subprocess.Popen(s_GZipCommand, shell=True)
                ary_PlannedActions.append("{0} GZipped.".format(s))
              except:
                ary_PlannedActions.append("Problem with the compression. {0}".format(s_GZipCommand)) 
              
              if config.debug:
                print "GZip command: {0} run.".format(s_GZipCommand) 

          
          if config.debug:
            print "File {0} exported. File size was {1}kB".format(s,i_FileSizeBefore)    
        
          ary_PlannedActions.append("Exported {0} to {1}. Size before compression: {2}kB".format(collection,config.s_OfflineCollectionPath+"/"+config.s_Dbase,i_FileSizeBefore))
          i_ExportCounter += 1     

        
        #if the user wanted to drop and it is ok to drop  
        if b_DropOK and b_Drop:
        
          try:
            m_Dbase.drop_collection(collection)
          except (ValueError, KeyError, TypeError) as e:
            ary_PlannedActions.append(e)
          else:
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