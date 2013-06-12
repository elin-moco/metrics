# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
from urlparse import urlparse


fx_path_pattern = re.compile('(/firefox/)([0-9a-z\.]+)/(whatsnew|firstrun|releasenotes)')
blog_path_pattern = re.compile('(/posts/[0-9]+)(/.*)')


def actual_path(url):
    #print url
    path = urlparse(url).path
    while path.endswith('/'):
        path = path[:-1]
    fxPathMatch = fx_path_pattern.search(path)
    if fxPathMatch:
        path = fxPathMatch.group(1) + fxPathMatch.group(3)
    blogPathMatch = blog_path_pattern.search(path)
    if blogPathMatch:
        path = blogPathMatch.group(1)
    print path
    if path == '':
        path = '/'
    return path


def main(argv = []):
    df = pd.read_hdf('mocotw.h5', 'fx_download')
    actualPathSeries = df['previousPagePath'].apply(actual_path)
    print actualPathSeries
    df['actualPagePath'] = actualPathSeries
    df.to_hdf('mocotw.h5', 'fx_download')

    df_sum = df[['actualPagePath', 'pageviews']].groupby('actualPagePath').sum().sort('pageviews', ascending=False)

    print df_sum

    df_sum.to_hdf('mocotw.h5', 'fx_download_sum')

    df_stack = df.groupby(['actualPagePath', 'date']).sum()
    df_stack = df_stack.reset_index()
    df_stack = df_stack[df_stack.actualPagePath.isin(df_sum[:10].index)]
    df_stack = df_stack.pivot(index='date', columns='actualPagePath', values='pageviews')
    df_stack = df_stack.fillna(0)
    df_stack = df_stack.reset_index()

    print df_stack

    df_stack.to_hdf('mocotw.h5', 'fx_download_stack')

