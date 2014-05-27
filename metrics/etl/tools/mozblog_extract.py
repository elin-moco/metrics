# -*- coding: utf-8 -*-
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
from metrics.settings import MOCO_API_SECRET, FFCLUB_API_SECRET, BEDROCK_GA_PROFILE, FFCLUB_GA_PROFILE

today = datetime.now().strftime('%Y-%m-%d')
POST_PATTERN = re.compile('^(/posts/[0-9]+)(.*)')
MOZBLOG_URL = 'http://blog.mozilla.com.tw'

def get_results(service):
    result = {}
    canonical = []
    page = 1
    limit = 5
    total = 100
    while ((page - 1) * limit) <= total:
        mozblogData = json.loads(urllib2.urlopen(
            '%s/api/get_recent_posts/?count=%d&page=%d' % (MOZBLOG_URL, limit, page)).read())
        total = mozblogData['count_total']
        for post in mozblogData['posts']:
            canonical += (post['url'], )
            result['/posts/%d' % post['id']] = {
                'id': post['id'],
                'title': post['title'],
                'thumbnail': post['thumbnail'] if 'thumbnail' in post else '',
                'comments': len(post['comments']),
                'date': post['date'].split(' ')[0],
                'fbShares': 0,
                'uniqueUsers': 0,
                'pageviews': 0,
            }
        page += 1

    urls = ','.join([x for x in canonical])
    fbShareData = json.loads(urllib2.urlopen('https://graph.facebook.com/?%s' % urlencode({'ids': urls})).read())

    for url, fbShare in fbShareData.items():
        pagePath = url[len(MOZBLOG_URL):url.rfind('/')]
        if 'shares' in fbShare:
            result[pagePath]['fbShares'] = fbShare['shares']
        # if 'comments' in fbShare:
        #     result[pagePath]['comments'] += fbShare['comments']

    rows = service.data().ga().get(
        ids='ga:' + BEDROCK_GA_PROFILE,
        start_date='2012-08-01',
        end_date=today,
        dimensions='ga:pagePath',
        metrics='ga:visitors,ga:pageviews',
        filters='ga:hostname==blog.mozilla.com.tw',
        sort='-ga:pageviews',
        max_results='1000'
    ).execute().get('rows')
    for row in rows:
        pagePath = row[0].encode('ascii', 'ignore')
        try:
            matchResult = POST_PATTERN.match(pagePath)
            if matchResult:
                realPath = matchResult.group(1)
                if realPath in result:
                    if 'uniqueUsers' in result[realPath]:
                        result[realPath]['uniqueUsers'] += int(row[1])
                    else:
                        result[realPath]['uniqueUsers'] = int(row[1])
                    if 'pageviews' in result[realPath]:
                        result[realPath]['pageviews'] += int(row[2])
                    else:
                        result[realPath]['pageviews'] = int(row[2])
        except IndexError:
            pass
    return result


def save_results(results):
    # Print data nicely for the user.
    if results:
        print '%d rows fetched' % len(results)
        newRows = {}
        for path, row in results.items():
            newRow = {}
            for prop, col in row.items():
                newRow[prop] = col.encode('utf8') if isinstance(col, (unicode, str)) else col
            print newRow
            newRows[path] = newRow
        df = pd.DataFrame(newRows).transpose()
        df = pd.DataFrame({
            'id': pd.Series(df['id'], dtype='int'),
            'title': pd.Series(df['title']),
            'date': pd.Series(df['date']).convert_objects(convert_dates='coerce'),
            'thumbnail': pd.Series(df['thumbnail']),
            'comments': pd.Series(df['comments'], dtype='int'),
            'fbShares': pd.Series(df['fbShares'], dtype='int'),
            'pageviews': pd.Series(df['pageviews'], dtype='int'),
            'uniqueUsers': pd.Series(df['uniqueUsers'], dtype='int')
        })
        # print df
        df.to_hdf('mozblog.h5', 'posts')
        # table = texttable.Texttable()
        # table.add_rows(newRows)
        # print table.draw()
        #print 'Total Visits: %s' % results.get('rows')[0][0]

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
