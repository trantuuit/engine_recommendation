"""
The standard CSVItemExporter class does not pass the kwargs through to the
CSV writer, resulting in EXPORT_FIELDS and EXPORT_ENCODING being ignored
(EXPORT_EMPTY is not used by CSV).
"""

from scrapy.conf import settings
from scrapy.contrib.exporter import CsvItemExporter
import io
import os
import six
import csv
from scrapy.extensions.feedexport import IFeedStorage
from w3lib.url import file_uri_to_path
from zope.interface import implementer



@implementer(IFeedStorage)
class FixedFileFeedStorage(object):

    def __init__(self, uri):
        self.path = file_uri_to_path(uri)

    def open(self, spider):
        dirname = os.path.dirname(self.path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
        return open(self.path, 'ab')

    def store(self, file):
        file.close()

class CSVkwItemExporter(CsvItemExporter):

    def __init__(self, file, include_headers_line=True, join_multivalued=',', **kwargs):
        kwargs['fields_to_export'] = settings.getlist('EXPORT_FIELDS') or None
        kwargs['encoding'] = settings.get('EXPORT_ENCODING', 'utf-8')

        super(CSVkwItemExporter, self).__init__(file, include_headers_line, join_multivalued, **kwargs)
        self._configure(kwargs, dont_fail=True)
        self.stream.close()
        storage = FixedFileFeedStorage(file.name)
        file = storage.open(file.name)
        self.stream = io.TextIOWrapper(
            file,
            line_buffering=False,
            write_through=True,
            encoding=self.encoding,
            newline='',
        ) if six.PY3 else file
        self.csv_writer = csv.writer(self.stream, **kwargs)

