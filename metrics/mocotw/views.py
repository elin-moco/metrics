"""Example views. Feel free to delete this app."""
import json

import logging
from django.http import HttpResponse
import pandas as pd
import numpy as np
from django.shortcuts import render

import commonware
from django.template.loader import render_to_string
from datetime import datetime

log = commonware.log.getLogger('playdoh')


def fx_download(request):
    """Main example view."""
    data = {}  # You'd add data here that you're sending to the template.
    return render(request, 'mocotw/fx_download.html', data)


def fx_download_sum_data(request):
    """Main example view."""
    df_sum = pd.read_hdf('mocotw.h5', 'fx_download_sum')
    arr_sum = []
    for pagePath, pageviews in df_sum[df_sum.pageviews > 200].itertuples():
        arr_sum += [{'pagePath': pagePath, 'pageviews': str(pageviews)}]
    return HttpResponse(json.dumps(arr_sum), mimetype='application/json')
    # return HttpResponse(render_to_string('dashboard/data.json'), mimetype='application/json')


def fx_download_stack_data(request):
    """Main example view."""
    df_stack = pd.read_hdf('mocotw.h5', 'fx_download_stack')
    arr_stack = []
    cols = df_stack.columns.tolist()
    for index, row in df_stack.iterrows():
        stack = {}
        for col in cols:
            val = row[col]
            if not isinstance(val, pd.datetime):
                stack[col] = int(val)
            else:
                stack[col] = val.strftime('%Y-%m-%d')

        arr_stack += [stack]
    return HttpResponse(json.dumps(arr_stack), mimetype='application/json')


def moztech_billboard(request):
    df_posts = pd.read_hdf('moztech.h5', 'posts')
    data = {'posts': df_posts.transpose().to_dict(), 'now': datetime.now()}
    return render(request, 'mocotw/moztech_billboard.html', data)


def mozblog_billboard(request):
    df_posts = pd.read_hdf('mozblog.h5', 'posts')
    data = {'posts': df_posts.transpose().to_dict(), 'now': datetime.now()}
    return render(request, 'mocotw/mozblog_billboard.html', data)


def newsletter_views(request):
    df_posts = pd.read_hdf('newsletter.h5', 'main')
    df_refers = pd.read_hdf('newsletter.h5', 'refers')
    dic = df_posts.transpose().to_dict()
    issues = {}
    for path, issue in dic.items():
        issue['refers'] = df_refers[df_refers['campaign'] == path[11:]].transpose().to_dict()
        issues[path] = issue

    data = {'issues': issues}
    return render(request, 'mocotw/newsletter_views.html', data)


def data(request):
    """Main example view."""
    return HttpResponse(render_to_string('dashboard/data.tsv'), mimetype='application/json')
