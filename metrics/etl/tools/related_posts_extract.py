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
from .file_cache import FileCache

MOZBLOG_URL = 'http://blog.mozilla.com.tw'


def get_results(service, profile_id):
    # Use the Analytics Service Object to query the Core Reporting API
    cache = FileCache()
    ga_visits = cache.get('ga-visits')
    if ga_visits is None:
        ga_visits = service.data().ga().get(
            ids='ga:' + profile_id,
            start_date='2012-08-01',
            end_date='2014-03-10',
            dimensions='ga:previousPagePath,ga:pagePath',
            metrics='ga:visitors',
            filters='ga:previousPagePath=~^/posts/.*;ga:pagePath=~^/posts/.*;ga:hostname==blog.mozilla.com.tw',
            #sort='',
            max_results='10000',
        ).execute()
        cache.set('ga-visits', ga_visits)

    # Print data nicely for the user.
    dataList = []
    outcome = []
    print 'Profile: %s' % ga_visits.get('profileInfo').get('profileName')
    rows = ga_visits.get('rows')
    print '%d rows fetched' % len(rows)
    #previousPage = results['rows'][0][0]
    #pagePath = results['rows'][0][1]
    #visitors = results['rows'][0][2]

    ###### results['rows'] -> [[prePagePath, pagePath, vistors], []]
    ###### dataList [[prePagePath, pagePath, visitor]]
    ###### merge the GA API
    for item in ga_visits['rows']:
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
    ####### Cache the Category API, DELTE the file per day
    # if((datetime.now() - fileTime).seconds > 86400):
    #     os.remove('json_data.txt')

    result = cache.get('post-relations')
    catList = cache.get('post-categories')
    if result is None or catList is None:
        ###### load the API
        page = 1
        limit = 5
        total = 1000
        catList = set()
        result = defaultdict(list)
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
                    catList.add(category['title'])
                #postTime[post['id']] = post['date']
            page += 1
            print catList
        catList = list(catList)
        cache.set('post-relations', result)
        cache.set('post-categories', catList)
    print result
    print 'result'

    #### Creat the category dictionary
    catToPostIdDict = defaultdict(list)
    for cat in catList:
        for postId, postCatList in result.items():
            if cat in postCatList:
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
    # with open('./check_all.json', 'w') as ff:
    #     for ii in outcome:
    #         for xx in ii:
    #             ff.write(str(xx))
    #         ff.write('=======')
    #     json.dump(outcome, ff)
    # print 'outcome'
    #print len(outcome)

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
        sumSubVisitors = group['visitors'].sum()
        group['confidence'] = group['visitors'].map(lambda x: x/sumSubVisitors)         
        return group

    dfOutcome = dfOutcome.reset_index() 
    dfOutcome = dfOutcome.groupby('pre').apply(myfunc)    
    dfOutcome = dfOutcome.set_index(['pre', 'next'])
    dfOutcome = dfOutcome.fillna(0)
    df_sort = dfOutcome.sort(['confidence'], ascending=[0])
    
    #### save the unique post id

    print df_sort.head()
    ##print len(df_sort)
    total_visitors = df_sort['visitors'].sum()
    df_sort['support'] = df_sort['visitors'] / total_visitors
    # sort the support 
    df_sort_by_support = df_sort[df_sort['support'] > df_sort[df_sort['support']>0]['support'].mean()]
    dfOutcome.to_hdf('relate_post.h5', 'outcome')
    df_sort.to_hdf('relate_post.h5', 'df_sort')
    df_sort_by_support.to_hdf('relate_post.h5', 'df_sort_by_support')
    print df_sort.head()
    #### Redis ####
    try:
        r_server = redis.Redis("localhost")
        print "here"
        print idList[0]
        r_server.sadd('mozblog-all-posts', *idList)
        ##  preId - {next : score} trnsform to redis
        sortedIdList = df_sort_by_support.reset_index()['pre'].unique().tolist()
        # add the confidence to sorted set
        for preIds in sortedIdList:
            preTmp = df_sort_by_support.loc[preIds]
            if preTmp is not None:
                addResultDict = preTmp['confidence'].to_dict()
                addResultDict = dict((str(k), v) for k, v in addResultDict.items())
                r_server.delete('mozblog-related-post-' + str(preIds))
                r_server.zadd('mozblog-related-post-' + str(preIds), **addResultDict)

        # add the category to the redis
        for catKey, catItemList in catToPostIdDict.items():
            r_server.delete('mozblog-category-%s' % catKey)
            r_server.sadd('mozblog-category-%s' % catKey, *catItemList)
        #print r_server.smembers('mozblog-category-Firefox')#('mozblog-category-Firefox', 1, 20)
    except RuntimeError as e:
        print e


def save_results(results):
    pass


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
            # save_results(results)

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
