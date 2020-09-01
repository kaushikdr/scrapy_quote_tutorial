# -*- coding: utf-8 -*-
import scrapy
import pdb
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from quotes.items import QuotesItem
from scrapy import signals
import csv
import time
from urllib.parse import urlparse

from pydispatch import dispatcher

class QuoteBotSpider(CrawlSpider):
    name = 'quotebot'
    download_delay = 0.5
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com/page/1/']
    rules = (
        Rule(
            LinkExtractor(allow=[r'/page/\d*']),
            callback='parse', follow=True
        ),
    )

    def __init__(self, *args, **kwargs):
        super(QuoteBotSpider, self).__init__(*args, **kwargs)
        self.failed_url = []
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)


    def clean_url(self, url):
        """
        This method will clean all possibile urls into a callable url
        """
        parsed_url = urlparse(url)

        return (parsed_url.scheme or 'https') + '://' + (
            parsed_url.netloc or 'quotes.toscrape.com') + parsed_url.path
    def parse(self, response):
        """
        This parsing will be used to get all quotes from a single page.
        """
        self.state['items_count'] = self.state.get('items_count', 0) + 1 
        quotes_list = response.xpath('/html/body/div/div[2]/div[1]/div')
        if not quotes_list:
            yield None

        for quote in quotes_list:
            item = QuotesItem()
            item['quote'] = quote.xpath('.//span[1]/text()')[0].extract()
            item['author'] = quote.xpath('.//span[2]/small/text()')[0].extract()
            tags = quote.xpath('.//div/a/text()').extract()
            item['tags'] = str(tags)
            author_url = quote.xpath('.//span[2]/a/@href')[0].extract()
            cleaned_author_url = self.clean_url(author_url)

            # Note the don't_filter parameter. It will allow for duplicate parsing  under this call.
            author_description_request = scrapy.Request(cleaned_author_url, callback=self.parse_author_detail_page, 
                meta={'item': item}, dont_filter=True, errback=self.handle_error)
            yield author_description_request


    def parse_author_detail_page(self, response):
        """
        This parsing will be used to get the born_information and the 
        description of the author from the 2nd page. 
        """
        self.state['items_count'] = self.state.get('items_count', 0) + 1
        item = response.meta['item']
        born_list = response.xpath('/html/body/div/div[2]/p[1]/span/text()').extract()
        item['born_information'] = " ".join(map(str, born_list))
        description = response.xpath('/html/body/div/div[2]/div/text()')[0].extract()
        item['author_description'] = description.strip()
        return item


    def handle_error(self, failure):
        """
        This method will be used to capture all the urls which could not be parsed 
        """
        url = failure.request.url
        self.failed_url.append({'failure_type': failure.type.__doc__, 'url': url})
        # logging.error('Failure type: %s, URL: %s', failure.type,
        #                                        url)
    
    def handle_spider_closed(self, spider, reason):
        # self.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
        if self.failed_url:
            keys = self.failed_url[0].keys()
            ERROR_FILE_NAME = "io_files/error_{}.csv".format(str(int(time.time())))
            with open(ERROR_FILE_NAME, 'w', newline='')  as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(self.failed_url)
            output_file.close()
