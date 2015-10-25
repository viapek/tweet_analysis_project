"""This script asks for a number of seconds and loops asking for a state decision.
When the state is set to problem then it performs pix actions then calls observers
so they can restart their processes
"""
import urllib2 as urllib
from urlparse import urlparse
import time

class jNetworkMonitor(object):

  PollTime = 10
  TargetAddress = 'http://google.com'

  def setTargetAddress(self, s):
      if not s.startswith('http://'):
        s_url = 'http://' + urlparse(s).netloc
      else:
        s_url = 'http://' + s
        
      self.TargetAddress = "http://" + s


  def getTargetAddress(self):
      return self.TargetAddress

  
  def __init__(self, **kwargs):
    self.observers = []
    for k in kwargs:
        try:
            setattr(self,k,kwargs[k])
        except:
            pass
    
        
  def grabPage(self):
    start_time = time.clock()
    try:
      res = urllib.urlopen(self.TargetAddress)
    except (ValueError, KeyError, TypeError) as e:
      self.__verifyNetwork(True)
      pass
    else:
      return "{0} retrieved at {1} in {2}".format(self.TargetAddress, time.ctime().split()[3], time.clock()-start_time)


  def __verifyNetwork(self, shitisbad):
    print "do some fancy network shit in here"
    if shitisbad:
      restartedNetwork = self.restartNetwork()
      
    if restartedNetwork:
      self.callAllWatchers
          
    print 'Happy Days from verifyNetwork'


  def restartNetwork(self):
    #TODO shutdown and restart the network service or refresh DHCP information
    print "Restart Network"



  def poller(self):
    
    while True:
      # Run our time.sleep() command,
      # and show the before and after time
      # print('Waiting %s seconds' % num)
      time.sleep(self.PollTime)

      try:
        self.__grabPage()
      except e:
        self.__verifyNetwork()
        pass
    
  def watchOutForMe(self, obj):
  #this registers the object and the method name to call
  #then check for network performance, mainly to address network resting
  #if there is a network problem this will call the s_methodName of the object
    if not obj in self.observers:
      print "Object {0} not in observers".format(obj)
      print self.observers
      try:
        getattr(obj, 'reconnect')
      except:
        raise ValueError  
      else:
        self.observers.append(obj)
        print "Watching out for you"
        print self.observers
    else:
      print "I'm already watching out for you already!!!"


  def endMyWatch(self, obj):
    if obj in self.observers:
      try:
        self.observers.remove(obj)
        print "My watch has ended"
        print self.observers
        pass
      except:
        raise ValueError
    else:
      print "{0} not in observers".format(obj)
  
  
  def callAllWatchers(self):
    for observer in self.observers:
      observer.reconnect()
    
    print "Happy days from callAllWatchers"



if __name__ == '__main__':
  netmon = jNetworkMonitor()
  
  print netmon.TargetAddress
  print netmon.PollTime
  
  #netmon.poller()

  
  #netmon.poller()