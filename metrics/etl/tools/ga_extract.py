#!/home/elin/.virtualenvs/metrics/bin/python
# -*- coding: utf-8 -*-

import sys

# import the Auth Helper class
import ga_auth

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
import texttable
import pandas as pd
import numpy as np


def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
    return service.data().ga().get(
        ids='ga:' + profile_id,
        start_date='2013-01-01',
        end_date='2013-06-10',
        dimensions='ga:date,ga:pagePath,ga:previousPagePath',
        metrics='ga:visitors,ga:pageviews,ga:timeOnPage',
        filters='ga:nextPagePath=~^/firefox/download/.*;ga:hostname=~^(blog\.|tech\.)?mozilla\.com\.tw',
        # filters='ga:campaign==epaper1303;ga:hostname=~^(blog\.|tech\.)?mozilla\.com\.tw',
        sort='-ga:pageviews',
        max_results='10000',
    ).execute()


def save_results(results):
    # Print data nicely for the user.
    if results:
        print 'Profile: %s' % results.get('profileInfo').get('profileName')
        rows = results.get('rows')
        # print '%d rows fetched' % len(rows)
        newRows = []
        for row in rows:
            newRow = []
            for col in row:
                newRow.append(col.encode('utf8'))
            newRows.append(newRow)
        dataArray = np.array(newRows)
        print 'array shape: %d, %d' % dataArray.shape
        df = pd.DataFrame({
            'date': pd.Series(dataArray[:, 0]).convert_objects(convert_dates='coerce'),
            'pagePath': pd.Series(dataArray[:, 1]),
            'previousPagePath': pd.Series(dataArray[:, 2]),
            'visitors': pd.Series(dataArray[:, 3], dtype='int'),
            'pageviews': pd.Series(dataArray[:, 4], dtype='int'),
            'timeOnPage': pd.Series(dataArray[:, 5], dtype='float')
        })
        df.to_hdf('mocotw.h5', 'fx_download')
        # table = texttable.Texttable()
        # table.add_rows(newRows)
        # print table.draw()
        #print 'Total Visits: %s' % results.get('rows')[0][0]

    else:
        print 'No results found'


def main(argv):
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

main(sys.argv)
