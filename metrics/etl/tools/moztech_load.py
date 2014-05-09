import pandas as pd
import json
import urllib2
import redis


def main():
    posts = pd.read_hdf('moztech.h5', 'posts')
    authors = pd.read_hdf('moztech.h5', 'authors')
    group = posts[['authorEmail', 'comments', 'fbShares', 'pageviews', 'uniqueUsers']].groupby('authorEmail')
    authorPosts = group.size()
    posts_max = group.max()
    authorEmails = authors['email']
    authorPostVisits = posts_max['pageviews']
    authorPostLikes = posts_max['fbShares']
    authorPostComments = posts_max['comments']
    nthPostApi = 'http://tech.mozilla.com.tw/api/get_posts/?count=1&page=%s&order_by=id&order=asc'
    authorNthPostBase = {'100': None, '200': None, '500': None, '1000': None}
    authorNthPost = {}
    for n in authorNthPostBase:
        data = json.loads(urllib2.urlopen(nthPostApi % n).read())
        if data['posts']:
            authorNthPost[n] = data['posts'][0]['author']['slug'].replace('mozilla-com', '@mozilla.com')

    print authorEmails
    print authorPosts
    print authorPostVisits
    print authorPostLikes
    print authorPostComments
    print authorNthPost

    try:
        redis_client = redis.Redis("localhost")

        redis_client.delete('moztech-author-emails')
        redis_client.delete('moztech-author-posts')
        redis_client.delete('moztech-author-post-visits')
        redis_client.delete('moztech-author-post-likes')
        redis_client.delete('moztech-author-post-comments')
        redis_client.delete('moztech-nth-post-author')

        redis_client.sadd('moztech-author-emails', *(authorEmails.tolist()))
        redis_client.zadd('moztech-author-posts', **authorPosts)
        redis_client.zadd('moztech-author-post-visits', **authorPostVisits)
        redis_client.zadd('moztech-author-post-likes', **authorPostLikes)
        redis_client.zadd('moztech-author-post-comments', **authorPostComments)
        redis_client.hmset('moztech-nth-post-author', authorNthPost)

    except RuntimeError as e:
        print e
