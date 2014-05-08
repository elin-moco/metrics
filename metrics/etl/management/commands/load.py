from django.core.management.base import BaseCommand
from metrics.etl.tools import related_posts_extract, fx_extract, log_extract, pd_transform, main_extract, moztech_extract, mozblog_extract, newsletter_extract, moztech_load
from metrics.settings import LOG_PATH


class Command(BaseCommand):
    help = 'Perform ETL cycle'

    def handle(self, *args, **options):
        if args is None:
            return
        if args[0] == 'log':
            log_extract.main((LOG_PATH, args[1]))
        if args[0] == 'fx':
            fx_extract.main()
            pd_transform.main()
        if args[0] == 'main':
            main_extract.main()
        if args[0] == 'moztech':
            moztech_extract.main()
            moztech_load.main()
        if args[0] == 'mozblog':
            mozblog_extract.main()
        if args[0] == 'newsletter':
            newsletter_extract.main()
        if args[0] == 'related_posts':
            related_posts_extract.main()
