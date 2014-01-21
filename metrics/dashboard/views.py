"""Example views. Feel free to delete this app."""
import json

import logging
from django.http import HttpResponse
import pandas as pd
from django.shortcuts import render

import commonware
from django.template.loader import render_to_string


log = commonware.log.getLogger('playdoh')


def home(request):
    import tables as _
    s = pd.read_hdf('dashboard.h5', 'user_counts')
    """Main example view."""
    data = {'user_counts': s.to_dict()}  # You'd add data here that you're sending to the template.
    return render(request, 'dashboard/home.html', data)
