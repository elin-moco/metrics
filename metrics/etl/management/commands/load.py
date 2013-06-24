from django.core.management.base import BaseCommand
from metrics.etl.tools import ga_extract, log_extract, pd_transform
from metrics.settings import LOG_PATH


class Command(BaseCommand):
    help = 'Perform ETL cycle'

    def handle(self, *args, **options):
        if args is None:
            return
        if args[0] == 'log':
            log_extract.main((LOG_PATH, args[1]))
        if args[0] == 'ga':
            ga_extract.main()
            pd_transform.main()
