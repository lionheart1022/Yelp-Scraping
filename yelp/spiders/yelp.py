# -*- coding: utf-8 -*-#
from __future__ import absolute_import, division, unicode_literals

import scrapy
import xlrd
from datetime import datetime
import time
import json
from scrapy.conf import settings


class YelpSpider(scrapy.Spider):
    name = "yelp_crawler"
    allowed_domains = ['www.yelp.com']
    start_urls = ['https://www.yelp.com/biz/premier-medical-associates-bushnell?osq=Premier+Medical+Associates']
    header = {
        'User-Agent': 'Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)'
    }

    settings.overrides['ROBOTSTXT_OBEY'] = False

    def start_requests(self):
        url_list = []
        file = xlrd.open_workbook("Research Offices.xlsx")
        sheet = file.sheet_by_index(0)
        for k in range(1, sheet.nrows):
            external_id = int(sheet.row_values(k)[0])
            url_list.append({'id': external_id, 'url': str(sheet.row_values(k)[9])})
        for url in url_list:
            if url['url'] and 'yelp.com' in url['url']:
                yield scrapy.Request(url=url['url'], callback=self.parse_product,
                                     dont_filter=True, headers=self.header,
                                     meta={'external_id': url['id']})

        # yield scrapy.Request(url=self.start_urls[0], callback=self.parse_product,
        #                      dont_filter=True, headers=self.header,
        #                      meta={'external_id': '1000000000'})

    def parse_product(self, response):
        review_list = []
        external_id = response.meta['external_id']

        review_contents_list = response.xpath('//div[@class="review-list"]/ul'
                                              '/li')[1:]
        for review_contents in review_contents_list:
            reviewer_name = review_contents.xpath('.//div[contains(@class, "review--with-sidebar")]'
                                                  '//li[@class="user-name"]/a/text()')[0].extract()

            photos = {
                'url': review_contents.xpath('.//div[@class="review-sidebar"]'
                                             '//img[@class="photo-box-img"]/@src')[0].extract(),
                'description': ''
            }

            stars = int(float(review_contents.xpath('.//div[@class="review-content"]'
                                                    '//div[contains(@class, "i-stars")]'
                                                    '/@title').re('(\d+\.\d+)')[0]))
            timestamp = review_contents.xpath('.//div[@class="review-content"]'
                                              '//span[@class="rating-qualifier"]/text()')[0].extract().strip()
            timestamp = datetime.strptime(timestamp, '%m/%d/%Y')
            timestamp = int(time.mktime(timestamp.timetuple())+timestamp.microsecond/1000000.0)

            content = review_contents.xpath('.//div[@class="review-content"]/p/text()').extract()
            content = '. '.join(content)

            reviews = {
                'provider': 'yelp',
                'reviewer_name': reviewer_name,
                'review_title': '',
                'content': content,
                'timestamp': timestamp,
                'stars': stars,
                'photos': photos
            }

            review_list.append(reviews)

        result = {
            'external_system_unique_id': external_id,
            'reviews': review_list
        }

        filename = 'output/' + 'reviews-' + str(external_id) + '.json'
        with open(filename, 'w+') as output:
            json.dump(result, output)
        yield
