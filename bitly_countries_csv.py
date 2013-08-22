import sys
import bitly_api
import os
import datetime
from datetime import date
from config import config
import time
from datetime import timedelta
import csv

#vars
load_date = date.today()

## Choose a specfici start/end date or just use yesterday (default)
#start_dt = date(2013,5,2) #custom dates
#end_dt = date(2013,5,2) #custom dates
diff = datetime.timedelta(days=-1)
start_dt = date.today() + diff
end_dt = start_dt


#funcs
def norm(s):
    if type(s) == unicode:
        return s.encode('utf-8').strip()
    else:
        return s

def nvl(s, r):
    if s is not None:
        return s
    else:
        return r

def keyCheck(key, arr, default):
    if key in arr.keys():
        return arr[key]
    else:
        return default

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

#open csv file for appending
fo = open("bitly_metrics.csv", "ab")
wr = csv.writer(fo, quoting=csv.QUOTE_ALL)


#connect to bitly
#you'll need to setup the ACCESS_TOKEN parameter in the config.py file first
conn_btly = bitly_api.Connection(access_token=config['ACCESS_TOKEN'])

## User either the popular_links function or specify your links in the
## array below. I've found the popular_links function sometimes causes
## errors so I switched to specifying my own list.

#pop_links = conn_btly.user_popular_links()
pop_links = [{'link':'http://mzl.la/downloadfirefox'}, {'link':'http://mzl.la/fx4and'}, {'link':'http://mzl.la/fx4and1'}, {'link':'http://mzl.la/153cPdR'}, {'link':'http://mzl.la/153cPKQ'}, {'link':'http://mzl.la/153dKeb'}, {'link':'http://mzl.la/12msZR9'}, {'link':'http://mzl.la/12mt1bB'}, {'link':'http://mzl.la/18RTKRa'}, {'link':'http://mzl.la/11JtPHe'}, {'link':'http://mzl.la/11JtUun'}, {'link':'http://mzl.la/18iQqOG'}, {'link':'http://mzl.la/14ymSYa'}, {'link':'http://mzl.la/11Ju1pU'}]

print str(len(pop_links)) + ' links to process'

print "###### Starting at " + str(datetime.datetime.now())
print "###### for dates " + str(start_dt) + " to " + str(end_dt)

while start_dt <= end_dt:

    #convert start_dt to epoch timestamp
    ts = int(time.mktime(start_dt.timetuple())) - time.timezone

    print "* " + str(start_dt)

    i=0
    #links loop
    for pop_link in pop_links:

        try:   

            i += 1

            print "*** Processing link " + str(i) + ": " + pop_link['link'] 

            #get link info
            link = conn_btly.link_info(pop_link['link'])

            #get country clicks for dates
            cc = conn_btly.link_countries(pop_link['link'], rollup=False, unit='day', unit_reference_ts=ts, units=1)
        
            #loop through country details
            for country in cc:

                r = [str(load_date), str(start_dt), pop_link['link'], keyCheck('country',country,'#unknown'), keyCheck('clicks',country,0)]
                wr.writerow(r)

        except:
            print "* couldn't load link: " + pop_link['link'] + " *"
            print "Unexpected error:", sys.exc_info()[0], sys.exc_info()[1]


    start_dt += datetime.timedelta(days=1)


#close the file        
fo.close()
print "###### Finished at " + str(datetime.datetime.now())
