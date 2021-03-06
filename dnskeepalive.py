"""This script asks for a number of seconds and a website address.

Then it will enter a loop to retrieve the address and will return the title 
and the time it took to retrieve the page.

I have written this to as a hack to keep my network interface active while
I sleep.
"""
import logging
import time
import urllib2 as urllib

#set some defaults
i_DefaultSeconds = 300
s_TargetWeb = "google.com"

def getHTMLTitle(s):
    # take a urllib2.open results and get the contents of <title></title>
    return s[s.find("<title>") + len("<title>"):s.find("</title>")]

 
def poller():
    global num, site

    while True:
        if not num:
            num = i_DefaultSeconds
        if not site:
            site = s_TargetWeb
        # Run our time.sleep() command,
        # and show the before and after time
        logging.debug('Waiting %d seconds before download', num)
        time.sleep(num)

        start_time = time.clock()
        try:
            res = urllib.urlopen(site)
        except (ValueError, KeyError, TypeError) as e:
            logging.debug("error - %s", e)
            pass
        else:
            logging.debug("%s : %s retrieved at %s in %s", site , getHTMLTitle(res.read()), time.ctime().split()[3], time.clock()-start_time)

  
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
