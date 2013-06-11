import tower


class DefaultLocaleMiddleware(object):
    """
    1. Search for the locale.
    2. Save it in the request.
    3. Strip them from the URL.
    """

    def process_request(self, request):
        request.locale = 'zh-TW'
        tower.activate('zh-TW')

