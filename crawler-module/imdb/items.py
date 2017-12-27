# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ImdbItem(scrapy.Item):
    Idx 			= scrapy.Field()
    Id 				= scrapy.Field()
    Title 			= scrapy.Field()
    Year 			= scrapy.Field()
    Genres 			= scrapy.Field()
    Directors		= scrapy.Field()
    Writers			= scrapy.Field()
    Actors			= scrapy.Field()
    Countries		= scrapy.Field()
    ReleaseDate		= scrapy.Field()
    ReleaseDate1	= scrapy.Field()
    ReleaseDate2	= scrapy.Field()
    Runtime			= scrapy.Field()
    Rating 			= scrapy.Field()
    RatingCount		= scrapy.Field()
    Popularity		= scrapy.Field()
    MetaScore		= scrapy.Field()
    PeopleMayLike	= scrapy.Field()
    Keywords		= scrapy.Field()
    Link			= scrapy.Field()
    Description		= scrapy.Field()
    image_urls		= scrapy.Field()
    images 			= scrapy.Field()
    file_urls		= scrapy.Field()
    files           = scrapy.Field()
    image_paths		= scrapy.Field()









