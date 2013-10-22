# -*- coding: utf-8 -*-
import json

import sys

# import the Auth Helper class
import urllib2
from datetime import datetime
import ga_auth

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
# import texttable
import pandas as pd
import numpy as np
from metrics.settings import MOCO_API_SECRET, FFCLUB_API_SECRET, BEDROCK_GA_PROFILE, FFCLUB_GA_PROFILE

hostMap = {
    'mozilla.com.tw': 'bedrockUniqueUsers',
    'blog.mozilla.com.tw': 'blogUniqueUsers',
    'tech.mozilla.com.tw': 'techUniqueUsers',
    'firefox.club.tw': 'ffclubUniqueUsers',
}
today = datetime.now().strftime('%Y-%m-%d')

def get_results(service):
    result = {}
    fbPageData = json.loads(urllib2.urlopen('https://graph.facebook.com/MozillaTaiwan').read())
    result['facebookFans'] = int(fbPageData['likes'])
    result['newsletterSubscriptions'] = \
        int(urllib2.urlopen('https://mozilla.com.tw/api/newsletter/subscriptions/count?secret=%s' % MOCO_API_SECRET).read())
    result['ffclubUserCount'] = \
        int(urllib2.urlopen('http://firefox.club.tw/api/users/registered/count?secret=%s' % FFCLUB_API_SECRET).read())
    result['ffclubFacebookUserCount'] = \
        int(urllib2.urlopen('http://firefox.club.tw/api/users/registered/facebook/count?secret=%s' % FFCLUB_API_SECRET).read())
    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        dimensions='ga:hostname',
        metrics='ga:visitors',
        sort='-ga:visitors',
        max_results='10',
    ).execute().get('rows')
    for row in rows:
        if row[0] in hostMap.keys():
            result[hostMap[row[0].encode('ascii', 'ignore')]] = int(row[1])
    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        # dimensions='ga:hostname',
        filters='ga:pagePath=~^/newsletter.*',
        metrics='ga:visitors',
        sort='-ga:visitors',
        max_results='10',
    ).execute().get('rows')
    for row in rows:
        result['newsletterWebUniqueUsers'] = int(row[0])
    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        # dimensions='ga:hostname',
        filters='ga:pagePath=~^/firefox/download.*',
        metrics='ga:visitors',
        sort='-ga:visitors',
        max_results='10',
    ).execute().get('rows')
    for row in rows:
        result['fxDownloadUniqueUsers'] = int(row[0])
    rows = service.data().ga().get(
        ids='ga:' + FFCLUB_GA_PROFILE,
        start_date='2013-04-01',
        end_date=today,
        dimensions='ga:hostname',
        metrics='ga:visitors',
        sort='-ga:visitors',
        max_results='10',
    ).execute().get('rows')
    for row in rows:
        if row[0] in hostMap.keys():
            result[hostMap[row[0].encode('ascii', 'ignore')]] = int(row[1])
    return result

def save_results(results):
    # Print data nicely for the user.
    if results:
        s = pd.Series(results.values(), results.keys())
        s.to_hdf('dashboard.h5', 'user_counts')
    else:
        print 'No results found'


def main(argv = []):
    # Step 1. Get an analytics service object.
    service = ga_auth.initialize_service()

    try:
        # Step 3. Query the Core Reporting API.
        results = get_results(service)
        print results
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
