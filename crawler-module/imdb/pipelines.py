# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request


class NoImagePipeline(object):
    def process_item(self, item, spider):
        if len(item['image_urls']) > 0:
            return item
        else:
            raise DropItem("Dropped! Movie have no image %s" % item)

class ImdbImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item

    def file_path(self, request, response=None, info=None):
        return request.meta.get('filename', '')

    def get_media_requests(self, item, info):
        meta = {'filename': item['Id'] + '.jpg'}
        if len(self.images_urls_field) > 0:
            return [Request(x, meta=meta) for x in item.get(self.images_urls_field, [])]
        return None


class ImdbFilePipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        return request.meta.get('filename', '')

    def get_media_requests(self, item, info):
        meta = {'filename': item['Id'] + '.mp4'}
        return [Request(x, meta=meta) for x in item.get(self.files_urls_field, [])]


        
