# -*- coding: utf-8 -*-

import sys

# import the Auth Helper class
import ga_auth

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
# import texttable
import pandas as pd
import numpy as np
import re

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
    #visitorsList = {}
    if results:
        print 'Profile: %s' % results.get('profileInfo').get('profileName')
        rows = results.get('rows')
        print '%d rows fetched' % len(rows)
        #previousPage = results['rows'][0][0]
        #pagePath = results['rows'][0][1]
        #visitors = results['rows'][0][2]
        
        #results['rows'] -> [[prePagePath, pagePath, vistors]]
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
    else:
        print 'No results found'
    print dataList
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
