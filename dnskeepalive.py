"""This script asks for a number of seconds and a website address.

Then it will enter a loop to retrieve the address and will return the title 
and the time it took to retrieve the page.

I have written this to as a hack to keep my network interface active while
I sleep.
"""
import time
import urllib2 as urllib

def getHTMLTitle(s):
    # take a urllib2.open results and get the contents of <title></title>
    return s[s.find("<title>") + len("<title>"):s.find("</title>")]

 
def sleeper():
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
            print site,": ",getHTMLTitle(res.read())," retrieved in ", time.clock()-start_time
    
# Get user input
num = input('How long to wait: ')
 
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
site = raw_input("Please enter a site address to call: ")
# simple url prep
if not site.startswith('http'):
    site = "http://" + site
 
try:
    sleeper()
except KeyboardInterrupt:
    print('\n\nKeyboard exception received. Exiting.')
    exit()
