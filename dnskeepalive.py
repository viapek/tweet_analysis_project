"""This script asks for a number of seconds and a website address.

Then it will enter a loop to retrieve the address and will return the title 
and the time it took to retrieve the page.

I have written this to as a hack to keep my network interface active while
I sleep.
"""
import time
import urllib2 as urllib

#set some defaults
i_DefaultSeconds = 300
s_TargetWeb = "google.com"

observers = []

def getHTMLTitle(s):
    # take a urllib2.open results and get the contents of <title></title>
    return s[s.find("<title>") + len("<title>"):s.find("</title>")]

 
def poller():
    while True:
        # Run our time.sleep() command,
        # and show the before and after time
        print('Waiting %s seconds' % num)
        time.sleep(num)

        start_time = time.clock()
        try:
            res = urllib.urlopen(site)
        except (ValueError, KeyError, TypeError) as e:
            print "error - "
            print e
            pass
        else:
            print site,": ",getHTMLTitle(res.read())," retrieved at ", time.ctime().split()[3], " in ", time.clock()-start_time

def watchOutForMe(obj, s_methodName):
  #this registers the object and the method name to call
  #then check for network performance, mainly to address network resting
  #if there is a network problem this will call the s_methodName of the object
  if not obj in observers:
      if obj.hasattr(s_methodName):
        obeservers.append([obj, s_methodName])
      else:
        raise ValueError
  #TODO store object in an array with the method name as a tuple
  
def endMyWatch(obj):
  #this deregisters the object
  #TODO check if the object exists
  if obj in observers:
  #TODO remove the object from the dictionary
    try:
        observers.remove(obj)
  #TODO return result
        pass
    else:
        raise ValueError

  
def callAllWatchers():
  #TODO go through the registered objects and call their methods
  for observer, method in observers:
      s_command = "{0}.{1}()".format(observer, method)
      print "Executing {0}".format(s_command)
      objCode = compile(s_command)
      exec(objCode)


def restartNetwork():
  #TODO shutdown and restart the network service or refresh DHCP information
  print "Restart Network"


if __name__ == '__main__':

  # Get user input
  try:
    num = input('How long to wait: [{0}] '.format(i_DefaultSeconds))
  except SyntaxError:
    num = i_DefaultSeconds

  # Try to convert it to a float
  while True:
    try:
        num = float(num)
    except ValueError:
        print('Please enter in a number.\n')
        continue
    else:
        break
    
  # get address
  site = raw_input("Please enter a site address to call: [{0}]".format(s_TargetWeb))
  if site == '':
    site = s_TargetWeb

  # simple url prep

  if not site.startswith('http'):
    site = "http://" + site
 
  try:
    poller()
  except KeyboardInterrupt:
    print('\n\nKeyboard exception received. Exiting.')
    exit()
