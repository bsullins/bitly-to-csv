import sys
import bitly_api
import os
import datetime
from datetime import date
from config import config
import time
from datetime import timedelta
import csv
import json
import urllib
from urllib import urlopen
import ast


#vars
load_date = date.today()

#start_dt = date(2014,3,3) #custom dates
#end_dt = date(2014,3,3) #custom dates
diff = datetime.timedelta(days=-7)
start_dt = date.today() + diff
end_dt = date.today()

tzOffset = "America/Los_Angeles"

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

#convert python dictionary (array) into json
#used to convert links from gist to json        
def link_list(list, prefix):
    lst=[]
    for l in list:
        links={}
        links[prefix]=l
        lst.append(links)
    return lst

def get_links():
    rawGist = urlopen('https://api.github.com/repos/bsullins/moco-bitly-links/git/blobs/80768c8444fba6767c38be5bd49130eee658600f')
    strGist = rawGist.read()
    dictGist = ast.literal_eval(strGist)
    encodedLinks = dictGist['content']
    decodedLinks = encodedLinks.decode('base64','strict').splitlines()
    links = link_list(decodedLinks, 'link')
    return links


pop_links = get_links()

#pop_links = conn_btly.user_popular_links()
## pop_links = [{'link':'http://mzl.la/downloadfirefox'}, {'link':'http://mzl.la/fx4and'}, {'link':'http://mzl.la/fx4and1'}, {'link':'http://mzl.la/153cPdR'}, {'link':'http://mzl.la/153cPKQ'}, {'link':'http://mzl.la/153dKeb'}, {'link':'http://mzl.la/12msZR9'}, {'link':'http://mzl.la/12mt1bB'}, {'link':'http://mzl.la/18RTKRa'}, {'link':'http://mzl.la/11JtPHe'}, {'link':'http://mzl.la/11JtUun'}, {'link':'http://mzl.la/18iQqOG'}, {'link':'http://mzl.la/14ymSYa'}, {'link':'https://bitly.com/142Q2kL'}, {'link':'http://mzl.la/1a98hsp'}, {'link':'http://mzl.la/16O2m6R'}, {'link':'http://mzl.la/16O3Ga2'}, {'link':'http://mzl.la/16Oc0Gx'}]
## snippets ## pop_links = [{'link':'http://mzl.la/1a98hsp'}, {'link':'http://mzl.la/16O2m6R'}, {'link':'http://mzl.la/16O3Ga2'}, {'link':'http://mzl.la/16Oc0Gx'}, {'link':'https://bitly.com/142Q2kL'}]
#pop_links = [{'link':'http://mzl.la/1a98hsp'}]

#create csv and write header row
header =  ['load_date', 'click_date', 'link', 'country', 'clicks']
fo = open("bitly_metrics_pst.csv",'ab')
#wr = csv.writer(fo)
wr = csv.DictWriter(fo, header)

#wr.writerow(header)


#connect to bitly
conn_btly = bitly_api.Connection(access_token=config['ACCESS_TOKEN'])


#print pop_links

print str(len(pop_links)) + ' links to process'

print "###### Starting at " + str(datetime.datetime.now())
print "###### for dates " + str(start_dt) + " to " + str(end_dt)

while start_dt <= end_dt:

    #convert start_dt to epoch timestamp
    ts = int(time.mktime(start_dt.timetuple())) - time.timezone

    print "* " + time.strftime("%m/%d/%y", time.localtime(ts))

    i=0
    #links loop
    for pop_link in pop_links: 

        try:

            print "*** Processing link " + str(i) + ": " + pop_link['link']

            #get link info
            link = conn_btly.link_info(pop_link['link'])

            #get country clicks for dates
            cc = conn_btly.link_countries(pop_link['link'], tz_offset=tzOffset, rollup=False, unit='day', unit_reference_ts=ts, units=1 )

            #row =[]
            
            #loop through country details
            for country in cc:
                rowData={}
                rowData['load_date']=load_date.strftime("%m/%d/%y")
                rowData['click_date']=time.strftime("%m/%d/%y", time.localtime(ts))
                rowData['link']=pop_link['link']
                rowData['country']=keyCheck('country',country,'#unknown')
                rowData['clicks']=keyCheck('clicks',country,0)

                #print country
                #print rowData
                #row.append(links)

                #print rowData
                #r = [load_date.strftime("%m/%d/%y"), start_dt.strftime("%m/%d/%y"), pop_link['link'], keyCheck('country',country,'#unknown'), keyCheck('clicks',country,0)]
                #print r
                wr.writerow(rowData)

            i += 1

            #write out details
            #print row
            #wr.writerow(row)
            
        except:
            print "* couldn't load link: " + pop_link['link'] + " due to error:"
            print sys.exc_info()[1]


    start_dt += datetime.timedelta(days=1)


        
fo.close()
print "###### Finished at " + str(datetime.datetime.now())









        
