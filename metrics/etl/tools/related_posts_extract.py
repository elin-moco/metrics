# -*- coding: utf-8 -*-
import sys
import os.path
import time
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
import math
import redis

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
        ###### merge the GA API
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
        
        ##print dataList
        print len(dataList)
        ###### below is the time and category API
        result = defaultdict(list)
        #postTime = {}
        canonical = []        
        page = 1
        limit = 5
        total = 1000
        
        ####### Cache the Category API, DELTE the file per day
        if(os.path.isfile('json_data.txt')):
            fileTime = datetime.strptime(time.ctime(os.path.getmtime('json_data.txt')), '%a %b %d %H:%M:%S %Y')
            if((datetime.now() - fileTime).seconds > 86400): 
                os.remove('json_data.txt')
            else:  
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
        print result
        print 'result'         
        with open('json_data.txt', 'w') as outfile:
            json.dump(result, outfile)
        
        #### Creat the category dictionary
        catToPostIdDict = defaultdict(list)
        catList = ['Firefox', 'Firefox for Android', 'Firefox OS', 'Firefox 教學影片', 'Firefox 祕技', 'Firefox 精選附加元件', 'Firefox 迷思', 'HTML5', 'Mozilla', 'Thunderbird', '新聞訊息', '未分類', '活動', '社群主打星']
        for cat in catList:
            for postId, postCatList in result.items():
                if(str(cat).decode('utf-8') in postCatList):
                    catToPostIdDict[cat].append(postId)
        print "分類:"
        print catToPostIdDict   
        
         
        # add the category +, -, time 
        ### dataList => [[pre, page, visitors]]
        ### item => [pre, page, visitors]
            #### result {id : category}
            #### key => id
        for preId in result:
            for nextId in result:
                # filter the preID == nextId
                if preId == nextId:
                    #print 'hit'
                    continue
                outList = []
                pos = 0
                neg = 0
                
                ### if the post is itself pos +1, 
                for categoryList in result[preId]:
                    if categoryList in result[nextId]:
                        pos += 1
                    # due to the timestamp also in the list so -2
                    # however if their categories are equal it will be -2
                    neg = len(set(result[nextId]) ^ set(result[preId])) - 2 
                outList.append(preId)
                outList.append(nextId)
                #outList.append(item[2])
                outList.append(result[nextId][0])
                outList.append(pos)
                outList.append(neg)    
                outcome.append(outList)                       
        ## To check the outcome correctly(debug used)
        with open('./check_all.json', 'w') as ff:
            for ii in outcome:
              for xx in ii:
                  ff.write(str(xx))
              ff.write('=======')
            #json.dump(outcome, ff)
        print 'outcome'
        #print len(outcome)    
    else:
        print 'No results found'
    
    ######## transfer the dataList to Data.frame
    dfOutcome = pd.DataFrame(outcome, dtype='float32', columns=('pre', 'next', 'time', 'pos', 'neg'))
    dfDataList = pd.DataFrame(dataList, columns=('pre', 'next', 'visitors'))
    dfOutcome = pd.DataFrame({
         'pre': pd.Series(dfOutcome['pre'], dtype='int'),
         'next': pd.Series(dfOutcome['next'], dtype='int'),
         'time': pd.Series(dfOutcome['time'], dtype='float32'),
         'pos': pd.Series(dfOutcome['pos'], dtype='int'),
         'neg': pd.Series(dfOutcome['neg'], dtype='int'),
    })
    dfDataList = pd.DataFrame({
         'pre': pd.Series(dfDataList['pre'], dtype='int'),
         'next': pd.Series(dfDataList['next'], dtype='int'),
         'visitors': pd.Series(dfDataList['visitors'], dtype='float32'),
    })
    dfOutcome = dfOutcome.set_index(['pre', 'next'])
    dfDataList = dfDataList.set_index(['pre', 'next'])
    ##print dfOutcome[1:50]
    ##print dfDataList[1:50]
    dfOutcome['visitors'] = dfDataList['visitors']

    dfOutcome = dfOutcome.fillna(0)
    ##print dfOutcome[1:50]
    ##print len(dfOutcome)
    dfOutcome = dfOutcome.reset_index()
    dfOutcome = dfOutcome[dfOutcome['pre'] != dfOutcome['next']]
    dfOutcome = dfOutcome.set_index(['pre', 'next'])
    
    dfOutcome['time'] = dfOutcome['time'] / float(10000)
    dfOutcome['score'] = dfOutcome['visitors'] + dfOutcome['pos'] - 0.5 * dfOutcome['neg']
    ##### count the score use the Association Rules #####
    idList = dfOutcome.reset_index()['pre'].unique()

    dfConfidence = pd.Series()
    for preId in idList:
        dfOutcome.ix[preId]['subSum'] = dfOutcome.ix[preId]['visitors'].sum()
    #    dfConfidence = dfConfidence.append(dfOutcome.loc[preId]['visitors'].map(lambda x: x/sumSubVisitors))
    def myfunc(group):
       # dfOutcome['subSum'] = group['visitors'].sum()
        group['next']
        print group.head()
       #for preId in idList:
             
            #group['confidence'] = group['next']['visitors'] / sumSubVisitors
        return group
    dfOutcome = dfOutcome.reset_index() 
    dfOutcome.groupby('pre').apply(myfunc)    
    dfOutcome = dfOutcome.set_index(['pre', 'next'])
     #dfOutcome['confidence'] = dfConfidence    
    #dfOutcome.loc[preId]['confidence'] = dfOutcome.loc[preId].loc[nexId]['visitors'] / dfOutcome.ix[preId]['visitors'].sum()

    df_sort = dfOutcome.sort(['score'], ascending=[0])
    print df_sort.head()
    ##print len(df_sort)
    total_visitors = df_sort['visitors'].sum()
    df_sort['support'] = df_sort['visitors'] / total_visitors
    dfOutcome.to_hdf('relate_post.h5', 'outcome')
    df_sort.to_hdf('relate_post.h5', 'df_sort')
    print df_sort.head()
    #### Redis ####
    try:
        r_server = redis.Redis("localhost")
    except RuntimeError as e:
        print e
    ##  preId - {next : score} trnsform to redis 
    for preIds in idList:
        preTmp = df_sort.ix[preIds]
        addResultDict = preTmp[0:-1]['score'].to_dict()
        #print addResultDict  
        addResultDict = dict((str(k), v) for k, v in addResultDict.items())
        r_server.zadd('mozblog-related-post-' + str(preIds), **addResultDict)
    ##print r_server.zrange('pre-post-1409', 1, 3000)    
    for catKey, catItemList in catToPostIdDict.items():
        sortCatList = sorted(catItemList, reverse=False)
        sortCatDict = dict((str(v), k) for k, v in enumerate(sortCatList))
        r_server.zadd('mozblog-category-' + str(catKey), **sortCatDict) 
    print r_server.zrange('mozblog-category-Firefox', 1, 20)

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
