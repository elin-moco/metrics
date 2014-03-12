# -*- coding: utf-8 -*-
import csv
import json
import re

import sys

# import the Auth Helper class
from urllib import urlencode
import urllib2
from datetime import datetime
import texttable
import ga_auth

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
# import texttable
import pandas as pd
import numpy as np
from metrics.settings import MOCO_API_SECRET, FFCLUB_API_SECRET, BEDROCK_GA_PROFILE, FFCLUB_GA_PROFILE, NEWSLETTER_FORMKEY
import requests
import StringIO

today = datetime.now().strftime('%Y-%m-%d')
CAMPAIGN_PATTERN = re.compile('^epaper([-0-9]+)')
ISSUE_PATTERN = re.compile('^(/newsletter/([-0-9]+)/)(.*)')
POST_PATTERN = re.compile('^(/posts/[0-9]+)(.*)')
MOZBLOG_URL = 'http://blog.mozilla.com.tw'


def get_results(service):
    result = {}

    url = 'https://docs.google.com/spreadsheet/ccc?key=' + NEWSLETTER_FORMKEY + '&output=csv'
    response = requests.get(url)

    df = pd.read_csv(StringIO.StringIO(response.content)).fillna(0)

    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        dimensions='ga:pagePath',
        metrics='ga:visitors,ga:pageviews,ga:avgTimeOnSite',
        filters='ga:pagePath=~^/newsletter/.*;ga:hostname==mozilla.com.tw',
        sort='-ga:pageviews',
        max_results='1000'
    ).execute().get('rows')

    for row in rows:
        pagePath = row[0].encode('ascii', 'ignore')
        try:
            matchResult = ISSUE_PATTERN.match(pagePath)
            if matchResult:
                realPath = matchResult.group(1)
                issue = matchResult.group(2)
                if realPath not in result:
                    if issue in df:
                        result[realPath] = {
                            'emailSent': int(df[issue][1]),
                            'emailViews': int(df[issue][3]),
                            'emailReply': int(df[issue][4]),
                            'extraDownloads': int(df[issue][11])
                        }
                    else:
                        result[realPath] = {}
                if pagePath.endswith('email') and issue >= '2013-10':
                    result[realPath]['emailViews'] = int(row[2])
                else:
                    if 'avgTimeOnSite' in result[realPath]:
                        result[realPath]['avgTimeOnSite'] = (result[realPath]['avgTimeOnSite'] * result[realPath]['pageViews'] + float(row[1]) * float(row[2])) / (result[realPath]['pageViews'] + float(row[2]))
                    else:
                        result[realPath]['avgTimeOnSite'] = float(row[3])
                    if 'uniqueUsers' in result[realPath]:
                        result[realPath]['uniqueUsers'] += int(row[1])
                    else:
                        result[realPath]['uniqueUsers'] = int(row[1])
                    if 'pageViews' in result[realPath]:
                        result[realPath]['pageViews'] += int(row[2])
                    else:
                        result[realPath]['pageViews'] = int(row[2])
        except IndexError:
            pass

    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        dimensions='ga:campaign',
        metrics='ga:visitors,ga:pageviews,ga:avgTimeOnSite',
        filters='ga:campaign=~^epaper.*;ga:pagePath!~^/newsletter/2.*;ga:hostname=~(blog\.|tech\.)?mozilla\.com\.tw',
        sort='-ga:pageviews',
        max_results='1000'
    ).execute().get('rows')

    for row in rows:
        pagePath = row[0].encode('ascii', 'ignore')
        try:
            matchResult = CAMPAIGN_PATTERN.match(pagePath)
            if matchResult:
                issue = matchResult.group(1)
                realPath = '/newsletter/20%s-%s/' % (issue[0:2], issue[2:4])
                result[realPath]['referUniqueUsers'] = int(row[1])
                result[realPath]['referPageViews'] = int(row[2])
                result[realPath]['referAvgTimeOnSite'] = float(row[3])
                pass
        except IndexError:
            pass

    result2 = {}

    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        dimensions='ga:hostname,ga:pagePath,ga:campaign',
        metrics='ga:visitors,ga:pageviews,ga:avgTimeOnSite',
        filters='ga:campaign=~^epaper.*;ga:pagePath!~^/newsletter/2.*;ga:hostname=~(blog\.|tech\.)?mozilla\.com\.tw',
        sort='-ga:pageviews',
        max_results='5000'
    ).execute().get('rows')

    for row in rows:
        hostname = row[0].encode('ascii', 'ignore')
        pagePath = row[1].encode('ascii', 'ignore').split('?')[0]
        campaign = row[2].encode('ascii', 'ignore')
        try:
            matchResult = POST_PATTERN.match(pagePath)
            if matchResult:
                pagePath = matchResult.group(1)
        except IndexError:
            pass

        try:
            matchResult = CAMPAIGN_PATTERN.match(campaign)
            if matchResult:
                issue = matchResult.group(1)
                campaign = '/20%s-%s/' % (issue[0:2], issue[2:4])
        except IndexError:
            pass
        pageUrl = hostname+pagePath
        if campaign not in result2:
            result2[campaign] = {}
        if pageUrl not in result2[campaign]:
            result2[campaign][pageUrl] = {}

        result2[campaign][pageUrl]['campaign'] = campaign
        result2[campaign][pageUrl]['page'] = pageUrl

        if 'uniqueUsers' in result2[campaign][pageUrl]:
            result2[campaign][pageUrl]['uniqueUsers'] += int(row[3])
        else:
            result2[campaign][pageUrl]['uniqueUsers'] = int(row[3])
        if 'pageViews' in result2[campaign][pageUrl]:
            result2[campaign][pageUrl]['pageViews'] += int(row[4])
        else:
            result2[campaign][pageUrl]['pageViews'] = int(row[4])
        if 'avgTimeOnSite' in result2[campaign][pageUrl]:
            result2[campaign][pageUrl]['avgTimeOnSite'] = (result2[campaign][pageUrl]['avgTimeOnSite']*result2[campaign][pageUrl]['pageViews'] + float(row[5])*int(row[4]))/(result2[campaign][pageUrl]['pageViews']+int(row[4]))
        else:
            result2[campaign][pageUrl]['avgTimeOnSite'] = float(row[5])

    refers = []

    for c, campaign in result2.items():
        for p, page in campaign.items():
            refers += [page, ]

    return {'main': result, 'refers': refers}


def save_results(results):
    # Print data nicely for the user.
    if results:
        print '%d issues fetched' % len(results['main'])
        df = pd.DataFrame(results['main']).fillna(0).transpose()

        df = pd.DataFrame({
            'emailSent': pd.Series(df['emailSent'], dtype='int'),
            'emailReply': pd.Series(df['emailReply'], dtype='int'),
            'emailViews': pd.Series(df['emailViews'], dtype='int'),
            'extraDownloads': pd.Series(df['extraDownloads'], dtype='int'),
            'pageViews': pd.Series(df['pageViews'], dtype='int'),
            'uniqueUsers': pd.Series(df['uniqueUsers'], dtype='int'),
            'avgTimeOnSite': pd.Series(df['avgTimeOnSite'], dtype='float'),
            'referPageViews': pd.Series(df['referPageViews'], dtype='int'),
            'referUniqueUsers': pd.Series(df['referUniqueUsers'], dtype='int'),
            'referAvgTimeOnSite': pd.Series(df['referAvgTimeOnSite'], dtype='float'),
        })
        df.to_hdf('newsletter.h5', 'main')

        print '%d refers fetched' % len(results['refers'])
        df = pd.DataFrame(results['refers']).fillna(0)
        df = pd.DataFrame({
            'campaign': pd.Series(df['campaign']),
            'page': pd.Series(df['page']),
            'pageViews': pd.Series(df['pageViews'], dtype='int'),
            'uniqueUsers': pd.Series(df['uniqueUsers'], dtype='int'),
            'avgTimeOnSite': pd.Series(df['avgTimeOnSite'], dtype='float'),
        })

        df.to_hdf('newsletter.h5', 'refers')

    else:
        print 'No results found'


def main():
    # Step 1. Get an analytics service object.
    service = ga_auth.initialize_service()

    try:

    # Step 3. Query the Core Reporting API.

        results = get_results(service)

        # for result, value in results.items():
        #     print result + ': ' + str(value)
        # print len(results)
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
