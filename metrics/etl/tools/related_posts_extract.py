# -*- coding: utf-8 -*-
import ga_auth
from collections import defaultdict

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
import pandas as pd
import re
import urllib2
import json
from datetime import datetime
import redis
from .file_cache import FileCache

MOZBLOG_URL = 'http://blog.mozilla.com.tw'
POST_PATTERN = re.compile('^(/posts/([0-9]+))(.*)')


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
    visitors_sheet = []
    category_sheet = []
    print 'Profile: %s' % ga_visits.get('profileInfo').get('profileName')
    ga_visits_data = ga_visits.get('rows')
    print '%d rows fetched' % len(ga_visits_data)

    #merge the GA data
    for ga_visit in ga_visits_data:
        prev_post_match = re.match(POST_PATTERN, ga_visit[0])
        next_post_match = re.match(POST_PATTERN, ga_visit[1])
        #visitors
        if prev_post_match and next_post_match and ga_visit[1]:
            try:
                prev_post_id = prev_post_match.group(2)
                next_post_id = next_post_match.group(2)
                for key, value in enumerate(visitors_sheet):
                    if prev_post_id == value[0] and next_post_id == value[1]:
                        visitors_sheet[key][2] += int(ga_visit[2])
                        break
                else:
                    visit_data = list()
                    visit_data.append(prev_post_id)
                    visit_data.append(next_post_id)
                    visit_data.append(int(ga_visit[2]))
                    visitors_sheet.append(visit_data)
            except IndexError as e:
                print e

    print len(visitors_sheet)

    post_data = cache.get('post-data')
    categories = cache.get('post-categories')
    category_posts = cache.get('category-posts')
    all_posts = cache.get('all-posts')
    if post_data is None or categories is None or category_posts is None or all_posts is None:
        ###### load the API
        page = 1
        limit = 5
        total = 1000
        categories = set()
        post_data = defaultdict(list)
        category_posts = defaultdict(list)
        all_posts = list()
        while((page - 1) * limit) <= total:
            posts_response = json.loads(urllib2.urlopen('%s/api/get_recent_posts/?count=%d&page=%d' % (MOZBLOG_URL, limit, page)).read())
            total = posts_response['count_total']
            for post in posts_response['posts']:
                ###### Append  date
                post_time = datetime.strptime(post['date'], '%Y-%m-%d %H:%M:%S')
                time_diff = (datetime.now() - post_time).seconds
                post_data[post['id']].append(time_diff)
                post_categories = [x['title'] for x in post['categories']]
                post_data[post['id']].extend(post_categories)
                categories |= set(post_categories)
                for post_category in post_categories:
                    category_posts[post_category].append(post['id'])
                all_posts.append(post['id'])
            page += 1
        categories = list(categories)
        cache.set('post-data', post_data)
        cache.set('post-categories', categories)
        cache.set('category-posts', category_posts)
        cache.set('all-posts', all_posts)
    print 'posts_data'
    print post_data
    print "category posts:"
    print category_posts
    print "post categories:"
    print categories

    #### Creat the category dictionary

    for prev_id in post_data:
        for next_id in post_data:
            # filter the preID == nextId
            if prev_id == next_id:
                #print 'hit'
                continue
            category_relation = []
            pos = len(set(post_data[next_id][1:]) & set(post_data[prev_id][1:]))
            neg = len(set(post_data[next_id][1:]) ^ set(post_data[prev_id][1:]))
            category_relation.append(prev_id)
            category_relation.append(next_id)
            category_relation.append(post_data[next_id][0])
            category_relation.append(pos)
            category_relation.append(neg)
            category_sheet.append(category_relation)

    ######## transform data list to DataFrame
    category_df = pd.DataFrame(category_sheet, dtype='float32', columns=('pre', 'next', 'time', 'pos', 'neg'))
    category_df = pd.DataFrame({
        'pre': pd.Series(category_df['pre'], dtype='int'),
        'next': pd.Series(category_df['next'], dtype='int'),
        'time': pd.Series(category_df['time'], dtype='float32'),
        'pos': pd.Series(category_df['pos'], dtype='int'),
        'neg': pd.Series(category_df['neg'], dtype='int'),
    })
    category_df = category_df.set_index(['pre', 'next'])

    visitors_df = pd.DataFrame(visitors_sheet, columns=('pre', 'next', 'visitors'))
    visitors_df = pd.DataFrame({
        'pre': pd.Series(visitors_df['pre'], dtype='int'),
        'next': pd.Series(visitors_df['next'], dtype='int'),
        'visitors': pd.Series(visitors_df['visitors'], dtype='float32'),
    })
    visitors_df = visitors_df.set_index(['pre', 'next'])

    category_df['visitors'] = visitors_df['visitors']
    category_df = category_df.reset_index()
    category_df = category_df[category_df['pre'] != category_df['next']]
    category_df = category_df.set_index(['pre', 'next'])
    
    category_df['time'] /= float(10000)
    category_df['score'] = category_df['visitors'] + category_df['pos'] - 0.5 * category_df['neg']

    ##### count the score use the Association Rules #####
    def calc_confidence(group):
        group_visitors_sum = group['visitors'].sum()
        group['confidence'] = group['visitors'].map(lambda c: c/group_visitors_sum)
        return group

    category_df = category_df.reset_index()
    category_df = category_df.groupby('pre').apply(calc_confidence)
    category_df = category_df.set_index(['pre', 'next'])
    category_df = category_df.fillna(0)
    # sort by confidence
    confidence_sorted_df = category_df.sort(['confidence'], ascending=[0])
    
    total_visitors = confidence_sorted_df['visitors'].sum()
    confidence_sorted_df['support'] = confidence_sorted_df['visitors'] / total_visitors
    # filter by support
    support_average = confidence_sorted_df[confidence_sorted_df['support'] > 0]['support'].mean()
    support_filtered_df = confidence_sorted_df[confidence_sorted_df['support'] > support_average]
    confidence_sorted_df.to_hdf('related_posts.h5', 'confidence_sorted')
    print confidence_sorted_df.head()

    #### Redis ####
    try:
        redis_client = redis.Redis("localhost")

        def add_to_redis(group):
            group_id = str(int(group.iloc[0]['pre']))
            confidence_dict = group.set_index('next')['confidence'].to_dict()
            confidence_dict = dict((str(k), v) for k, v in confidence_dict.items())
            redis_client.delete('mozblog-related-post-%s' % group_id)
            redis_client.zadd('mozblog-related-post-%s' % group_id, **confidence_dict)
            return group
        support_filtered_df.reset_index().groupby('pre').apply(add_to_redis)

        for category, posts in category_posts.items():
            redis_client.delete('mozblog-category-%s' % category)
            redis_client.sadd('mozblog-category-%s' % category, *posts)

        redis_client.delete('mozblog-all-posts')
        redis_client.sadd('mozblog-all-posts', *all_posts)
    except RuntimeError as e:
        print e


def main(argv = []):
    # Step 1. Get an analytics service object.
    service = ga_auth.initialize_service()

    try:
        # Step 2. Get the user's first profile ID.
        profile_id = '55777084'

        if profile_id:
            # Step 3. Query the Core Reporting API.
            get_results(service, profile_id)
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
