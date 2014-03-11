from django.conf.urls.defaults import *

from . import views


urlpatterns = patterns('',
    url(r'^fx_download$', views.fx_download, name='mocotw.fx_download'),
    url(r'^mozblog_billboard', views.mozblog_billboard, name='mocotw.mozblog_billboard'),
    url(r'^moztech_billboard', views.moztech_billboard, name='mocotw.moztech_billboard'),
    url(r'^newsletter_views', views.newsletter_views, name='mocotw.newsletter_views'),
    url(r'^data.tsv$', views.data, name='mocotw.data'),
    url(r'^fx_download_sum.json$', views.fx_download_sum_data, name='mocotw.fx_download_sum.data'),
    url(r'^fx_download_stack.json$', views.fx_download_stack_data, name='mocotw.fx_download_stack.data'),
)
