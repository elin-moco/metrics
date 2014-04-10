# -*- coding: utf-8 -*-
import sys
import os.path
# import the Auth Helper class
import ga_auth
from collections import defaultdict

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
# import texttable
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import urllib2
import json
from datetime import datetime

MOZBLOG_URL = 'http://blog.mozilla.com.tw'

def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
    return service.data().ga().get(
        #ids='ga:' + profile_id,
        #start_date='2012-08-01',
        #end_date='2013-06-10',
        #dimensions='ga:date,ga:pagePath,ga:previousPagePath',
        #metrics='ga:visitors,ga:pageviews,ga:timeOnPage',
        #filters='ga:nextPagePath=~^/firefox/download/.*;ga:hostname=~^(blog\.|tech\.)?mozilla\.com\.tw',
        # filters='ga:campaign==epaper1303;ga:hostname=~^(blog\.|tech\.)?mozilla\.com\.tw',
        #sort='-ga:pageviews',
        #max_results='10000',

        ids='ga:' + profile_id,
        start_date='2012-08-01',
        end_date='2014-03-10',
        dimensions='ga:previousPagePath,ga:pagePath',
        metrics='ga:visitors',
        filters='ga:previousPagePath=~^/posts/.*;ga:pagePath=~^/posts/.*;ga:hostname==blog.mozilla.com.tw',
        #sort='',
        max_results='10000',
    ).execute()


def save_results(results):
    # Print data nicely for the user.
    mergePage = []
    dataList = []
    result = {}
    outcome = []
    #visitorsList = {}
    if results:
        print 'Profile: %s' % results.get('profileInfo').get('profileName')
        rows = results.get('rows')
        print '%d rows fetched' % len(rows)
        #previousPage = results['rows'][0][0]
        #pagePath = results['rows'][0][1]
        #visitors = results['rows'][0][2]
        
        ###### results['rows'] -> [[prePagePath, pagePath, vistors], []]
        ###### dataList [[prePagePath, pagePath, visitor]]
        for item in results['rows']:
            mergePage = []
            previousPage = re.match(r"^(/posts/([0-9]+))(.*)", item[0])
            pagePath = re.match(r"^(/posts/([0-9]+))(.*)", item[1])
            #visitors
            try:
                if previousPage and pagePath and item[1]:
                    #print reg.group(2)
                    for key, value in enumerate(dataList):  
                        if previousPage.group(2) == value[0] and pagePath.group(2) == value[1]:   
                            dataList[key][2] += int(item[2])
                            break
                    else:  
                        mergePage.append(previousPage.group(2))
                        mergePage.append(pagePath.group(2))
                        mergePage.append(int(item[2]))
                        dataList.append(mergePage)
            except RuntimeError as e:
                print e
# dataList sample [pre, ref, visitors] => [u'994', u'994', 24]
        
        #print dataList
        ###### below is the time and category API
        result = defaultdict(list)
        #postTime = {}
        canonical = []        
        page = 1
        limit = 5
        total = 1000
        
        ####### Cache
        if(os.path.isfile('json_data.txt')):  
            with open('json_data.txt') as f:
                result = json.loads(f.read())
        else:    
            ###### load the API
            while((page - 1) * limit) <= total:
                relData = json.loads(urllib2.urlopen('%s/api/get_recent_posts/?count=%d&page=%d' % (MOZBLOG_URL, limit, page)).read())                  
                total = relData['count_total'] 
                for post in relData['posts']:
                    ###### Append  date
                    timeStamp = 0
                    dateObject = datetime.strptime(post['date'], '%Y-%m-%d %H:%M:%S') 
                    timeStamp = (datetime.now() - dateObject).seconds 
                    result[post['id']].append(timeStamp)
                    for category in post['categories']:
                        result[post['id']].append(category['title'])
                    #postTime[post['id']] = post['date']
                page += 1
        #print result         
        with open('json_data.txt', 'w') as outfile:
            json.dump(result, outfile)
            
        # add the category +, -, time 
        ### dataList => [[pre, page, visitors]]
        ### item => [pre, page, visitors]
            #### result {id : category}
            #### key => id
        for preId in result:
            for nextId in result:
                if preId == nextId:
                    continue
                outList = []
                pos = 0
                neg = 0
                
                ### if the post is itself pos +1
                for categoryList in result[preId]:
                    if categoryList in result[nextId]:
                        pos += 1
                    neg = len(set(result[nextId]) ^ set(result[preId])) - 2 
                outList.append(preId)
                outList.append(nextId)
                #outList.append(item[2])
                outList.append(result[nextId][0])
                outList.append(pos)
                outList.append(neg)    
                outcome.append(outList)                       
                    #else:
                        #for c in result[k]:
                        #    if c in result[kk]:
                        #        pos += 1
                        #neg = len(set(result[k]) ^ set(result[kk])) - 2 
                        #outList.append(k)
                        #outList.append(kk)
                        #outList.append(0)
                        #outList.append(result[kk][0])
                        #outList.append(pos)
                        #outList.append(neg)
                        #outcome.append(outList)        
        
        with open('./check_all.json', 'w') as ff:
            json.dump(outcome, ff)
        #print outcome
        #print dataList
        #print len(outcome) 
            #    if item[0] == key:                    
            #        item.append(timeStamp)
            #        for c in result[item[0]]:
            #            if c in result[item[1]]:
            #                pos += 1
            #        item.append(pos)
            #if item[0] in result:    
            #    item.append(len(set(result[item[0]]) ^ set(result[item[1]])))
      
            
            #    print "time!"
            #    item.append(postTime[item[1]])
            
            #pos = 0 
        #print dataList 
    else:
        print 'No results found'
    
    ######## transfer the dataList to Data.frame
    dfOutcome = pd.DataFrame(outcome, dtype='float32', columns=('pre', 'next', 'time', 'pos', 'neg'))
    dfDataList = pd.DataFrame(dataList, columns=('pre', 'next', 'visitors'))
     
    df = pd.merge(dfOutcome, dfDataList, how='outer')#, on=('pre', 'next')) 
    df = df.fillna(0)
    print df[1:30]
    df = df.set_index(['pre', 'next'])
    df['time'] = df['time'] / float(10000)
    df['pos'] = df['pos']
    df['visitors'] = df['visitors']
    df['score'] = df['visitors']/float(10) + df['pos'] - df['neg']
    #df_final = df.sort(columns='time', ascending=True)
    #print dfOutcome.head()
    #print dfDataList.head()
    print df[20000:20050] 
    #print df_final[1:10]
    df.to_hdf('relate_post.h5', 'raw_data')

def main(argv = []):
    # Step 1. Get an analytics service object.
    service = ga_auth.initialize_service()

    try:
        # Step 2. Get the user's first profile ID.
        profile_id = '55777084'

        if profile_id:
            # Step 3. Query the Core Reporting API.
            results = get_results(service, profile_id)
            # Step 4. Output the results.
            save_results(results)

    except TypeError, error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)

    except HttpError, error:
        # Handle API errors.
        print ('Arg, there was an API error : %s : %s' %
               (error.resp.status, error._get_reason()))

    except AccessTokenRefreshError:
        # Handle Auth errors.
        print ('The credentials have been revoked or expired, please re-run '
               'the application to re-authorize')

# main(sys.argv)
