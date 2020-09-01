# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
from scrapy.utils.project import get_project_settings

class QuotesPipeline(CsvItemExporter):
    """
    This pipeline will be used to process the spider to extract csv file. 
    """
    def __init__(self, *args, **kwargs):
        # try:
        file_name = get_project_settings().get('FILE_NAME', 'OUTPUT.csv')


        self.file = open(file_name, 'wb')

        kwargs['file'] = self.file
        delimiter = get_project_settings().get('CSV_DELIMITER', ',')
        kwargs['delimiter'] = delimiter

        fields_to_export = get_project_settings().get('FIELDS_TO_EXPORT', [])
        if fields_to_export:
            kwargs['fields_to_export'] = fields_to_export

        super(QuotesPipeline, self).__init__(*args, **kwargs)
        self.start_exporting()

    def close_spider(self, spider):
        self.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.export_item(item)
        return item
