from dircache import listdir
from datetime import datetime
import importlib
import texttable


def main(argv = []):
    # config = __import__('metrics.etl.tools', globals(), locals(), ['log_config_' + argv[1]], -1)
    config = importlib.import_module('metrics.etl.tools.log_config_' + argv[1])
    print config
    headerParam = config.headerParam
    wallpaperAPrefix = config.wallpaperAPrefix
    wallpaperBPrefix = config.wallpaperBPrefix
    dateRange = config.dateRange

    dir = argv[0]
    dateFormat = '%Y-%m-%d'
    fromDate = datetime.strptime(dateRange[0], dateFormat)
    toDate = datetime.strptime(dateRange[1], dateFormat)


    def filter_logs(log):
        if log.startswith('access.log.'):
            date = datetime.fromtimestamp(int(log[11:]))
            if fromDate < date < toDate:
                return True
        return False

    logs = [log for log in listdir(dir) if filter_logs(log)]
    headerAccess = []
    wallpaperA1920Download = []
    wallpaperA1366Download = []
    wallpaperB1920Download = []
    wallpaperB1366Download = []

    for log in logs:
        with open(dir + log) as logFile:
            dateKey = datetime.fromtimestamp(int(log[11:])).strftime(dateFormat)
            print 'parsing ' + dateKey
            headerAccessCount = 0
            wallpaperA1920DownloadCount = 0
            wallpaperA1366DownloadCount = 0
            wallpaperB1920DownloadCount = 0
            wallpaperB1366DownloadCount = 0
            for line in iter(logFile):
                if headerParam in line:
                    headerAccessCount += 1
                if wallpaperAPrefix + '-1920' in line:
                    wallpaperA1920DownloadCount += 1
                if wallpaperAPrefix + '-1366' in line:
                    wallpaperA1366DownloadCount += 1
                if wallpaperBPrefix + '-1920' in line:
                    wallpaperB1920DownloadCount += 1
                if wallpaperBPrefix + '-1366' in line:
                    wallpaperB1366DownloadCount += 1
            headerAccess += ((dateKey, headerAccessCount), )
            wallpaperA1920Download += ((dateKey, wallpaperA1920DownloadCount), )
            wallpaperA1366Download += ((dateKey, wallpaperA1366DownloadCount), )
            wallpaperB1920Download += ((dateKey, wallpaperB1920DownloadCount), )
            wallpaperB1366Download += ((dateKey, wallpaperB1366DownloadCount), )

    merge = lambda x, y: (x[0], x[1] + y[1])
    sum = lambda x, y: (x[0], x[1] + y[1])
    wallpaperADownload = map(merge, wallpaperA1920Download, wallpaperA1366Download)
    wallpaperBDownload = map(merge, wallpaperB1920Download, wallpaperB1366Download)

    table = texttable.Texttable()
    table.add_rows(headerAccess)
    print 'headerAccess'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperA1920Download)
    print 'wallpaperA1920Download'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperA1366Download)
    print 'wallpaperA1366Download'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperB1920Download)
    print 'wallpaperB1920Download'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperB1366Download)
    print 'wallpaperB1366Download'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperADownload)
    print 'wallpaperADownload'
    print table.draw()
    table.reset()
    table.add_rows(wallpaperBDownload)
    print 'wallpaperBDownload'
    print table.draw()

    print reduce(sum, headerAccess, ('headerAccessTotal', 0))
    print reduce(sum, wallpaperA1920Download, ('wallpaperA1920DownloadTotal', 0))
    print reduce(sum, wallpaperA1366Download, ('wallpaperA1366DownloadTotal', 0))
    print reduce(sum, wallpaperB1920Download, ('wallpaperB1920DownloadTotal', 0))
    print reduce(sum, wallpaperB1366Download, ('wallpaperB1366DownloadTotal', 0))
    print reduce(sum, wallpaperADownload, ('wallpaperADownloadTotal', 0))
    print reduce(sum, wallpaperBDownload, ('wallpaperBDownloadTotal', 0))
