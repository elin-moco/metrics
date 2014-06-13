import json
import os


class FileCache(object):

    @staticmethod
    def get(key):
        if os.path.isfile('%s.cache.json' % key):
            with open('%s.cache.json' % key) as f:
                return json.loads(f.read())
        return None

    @staticmethod
    def set(key, value):
        with open('%s.cache.json' % key, 'w') as f:
            json.dump(value, f, indent=4)
