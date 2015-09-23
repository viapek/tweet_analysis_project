import config
import glob
import subprocess
import time
import os
from pymongo import MongoClient

##initiate counters at 0
i_ImportCounter = 0
i_TotalDataSize = 0
i_TotalCollectionDataSize = 0
i_UnzipCounter = 0
i_ReZipCounter = 0

def getUserInput():
  while True:

    d_StartDate = raw_input("Please enter the START date in MMYY format, [{0}] ".format(time.strftime('%m%d', time.gmtime())))

    try:
      int(d_StartDate)
      len(d_StartDate) == 4
    except:
      continue

    d_EndDate = raw_input("Please enter the END date in MMYY format ")

    try:
      int(d_EndDate)
      len(d_EndDate) == 4
    except:
      continue
  
    if not int(d_StartDate) <= int(d_EndDate):
      print "The start date must be earlier or equal to the end date"
      continue
  
    b_Confirmation = raw_input("All files from {0} to {1} will be restored. [Y/N]".format(d_StartDate, d_EndDate))
    if b_Confirmation == "Y":
      return (d_StartDate, d_EndDate)

def getFileListByExtn(s_extn):
  #build a string from the congi output location and *. s_extn
  s_FileMatcher = config.s_OfflineCollectionPath + "/{0}/*{1}".format(config.s_Dbase,s_extn)
  #return the glob
  return glob.glob(s_FileMatcher)
  

def GZip(s_filesrc, b_Zip_Unzip):
  #this takes the file full path and boolean. True is zip, false is unzip
  if b_Zip_Unzip:
    s_GZipCommand = "gzip -9 {0}".format(s_filesrc)
  else:
    s_GZipCommand = "gzip -d {0}".format(s_filesrc)

  proc = subprocess.Popen(s_GZipCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  
  #for line in proc.stdout.readlines():
  #  print line

  if proc.wait() == 0:
    #if the return code is 0 we can go 
    if b_Zip_Unzip:
      ary_PlannedActions.append("GZipped: {0}".format(s_filesrc))
    else:
      ary_PlannedActions.append("GUnzipped: {0}".format(s_filesrc))
    return True
  else:
    #otherwise log it and do not drop the table
    if b_Zip_Unzip:
      ary_PlannedActions.append("Problem with GZip {0}".format(s_filesrc))
    else:
      ary_PlannedActions.append("Problem with GUnzipped {0}".format(s_filesrc))
    return False

def doMongoImport(s_filesrc):
  s_ImportCommand = "mongorestore -u {0} -p {1} -d {2} -c {3} {4}".format(
                config.s_MongoUser,
                config.s_MongoPassword,
                config.s_WorkingDatabase,
                config.s_WorkingCollection,
                s_filesrc)
          
  if config.debug:
    print s_ImportCommand
           
  #excute the command and get feedback
  proc = subprocess.Popen(s_ImportCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  #for line in proc.stdout.readlines():
  #    print line

  if proc.wait() == 0:
    #if the return code is 0 we can go 
    ary_PlannedActions.append("Should be loaded: {0}".format(s_filesrc))
    return True
  else:
    #otherwise log it and do not drop the table
    ary_PlannedActions.append("Failed to import {0}".format(s_filesrc))
    return False       
              
              

if __name__ == '__main__':
  #setup an array for tracking actions
  range = getUserInput()

  ary_PlannedActions = []
    
  files = sorted(getFileListByExtn("bson.gz"))
    
  for f in files:

    if int(f.split("/")[5][0:4]) >= int(range[0]) and int(f.split("/")[5][0:4]) <= int(range[1]):
      #create our gzip string
      if GZip(f, False):
        i_UnzipCounter += 1
        b_Import = True

      if b_Import:
        #build the mongodump command string
        if doMongoImport(f[:-3]):
          i_ImportCounter += 1          

      if GZip(f[:-3], True):
        i_ReZipCounter +=1
        
    else:
      if config.debug:
        print "{0} is out of range {1}".format(int(f.split("/")[5][0:4]),range)
       
            
  if config.debug:
    print ary_PlannedActions,"\n"
    
  print "{0} files evaluated. \n".format(len(files))
  print "- {0} successfully unzipped.".format(i_UnzipCounter) 
  print "- {0} successfully imported.".format(i_ImportCounter)
  print "- {0} successfully rezipped.".format(i_ReZipCounter)
  print "\n"
else:
  print "Problems starting the application. Please let me know"