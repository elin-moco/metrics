from django.core.management.base import BaseCommand
from metrics.etl.tools import related_posts_extract, fx_extract, log_extract, pd_transform, main_extract, moztech_extract, mozblog_extract, newsletter_extract, moztech_load, browser_survey_extract
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
            if len(args) > 1:
                related_posts_extract.main(args[1])
            else:
                related_posts_extract.main('blog')
                related_posts_extract.main('tech')
        if args[0] == 'browser_survey':
            browser_survey_extract.main()
