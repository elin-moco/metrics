from django.core.management.base import NoArgsCommand
from metrics.etl.tools import ga_extract, log_extract, pd_transform
import os

class Command(NoArgsCommand):
    help = 'Perform ETL cycle'
    option_list = NoArgsCommand.option_list

    def handle_noargs(self, **options):
        self.options = options
        ga_extract.main()
        pd_transform.main()


