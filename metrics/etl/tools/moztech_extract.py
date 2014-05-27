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
from metrics.settings import MOCO_API_SECRET, FFCLUB_API_SECRET, BEDROCK_GA_PROFILE, FFCLUB_GA_PROFILE, MOZTECH_AUTHORS_FILE

today = datetime.now().strftime('%Y-%m-%d')
POST_PATTERN = re.compile('^(/posts/[0-9]+)(.*)')
MOZTECH_URL = 'http://tech.mozilla.com.tw'


def get_results(service):

    authors = pd.DataFrame.from_csv(MOZTECH_AUTHORS_FILE, header=-1, parse_dates=False)
    authors = authors.reset_index()
    authors.columns = ['id', 'email', 'name']
    authors = authors.set_index('id')
    authors['name'] = authors['name'].map(lambda x: x.strip())
    authors['email'] = authors['email'].map(lambda x: x.strip())

    result = {}
    canonical = []
    page = 1
    limit = 20
    total = 100
    while ((page - 1) * limit) <= total:
        moztechData = json.loads(urllib2.urlopen(
            '%s/api/get_recent_posts/?count=%d&page=%d' % (MOZTECH_URL, limit, page)).read())
        total = moztechData['count_total']
        for post in moztechData['posts']:
            canonical += (post['url'], )
            author_id = post['author']['id']
            result['/posts/%d' % post['id']] = {
                'id': post['id'],
                'title': post['title'],
                'thumbnail': post['thumbnail'] if 'thumbnail' in post else '',
                'authorEmail': authors.loc[author_id]['email'],
                'authorName': authors.loc[author_id]['name'],
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
        pagePath = url[len(MOZTECH_URL):url.rfind('/')]
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
        filters='ga:hostname==tech.mozilla.com.tw',
        sort='-ga:pageviews',
        max_results='1000',
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
    return {'authors': authors, 'posts': result}


def save_results(results):
    # Print data nicely for the user.
    if 'authors' in results:
        results['authors'].to_hdf('moztech.h5', 'authors')

    if 'posts' in results:
        posts = results['posts']
        print '%d rows fetched' % len(posts)
        newRows = {}
        for path, row in posts.items():
            newRow = {}
            for prop, col in row.items():
                newRow[prop] = col.encode('utf8') if isinstance(col, unicode) else col
            print newRow
            newRows[path] = newRow
        df = pd.DataFrame(newRows).transpose()
        df = pd.DataFrame({
            'id': pd.Series(df['id'], dtype='int'),
            'title': pd.Series(df['title']),
            'date': pd.Series(df['date']).convert_objects(convert_dates='coerce'),
            'thumbnail': pd.Series(df['thumbnail']),
            'authorEmail': pd.Series(df['authorEmail']),
            'authorName': pd.Series(df['authorName']),
            'comments': pd.Series(df['comments'], dtype='int'),
            'fbShares': pd.Series(df['fbShares'], dtype='int'),
            'pageviews': pd.Series(df['pageviews'], dtype='int'),
            'uniqueUsers': pd.Series(df['uniqueUsers'], dtype='int')
        })
        # print df
        df.to_hdf('moztech.h5', 'posts')
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
