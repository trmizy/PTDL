# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BookstoreItem(scrapy.Item):
     Ten_Sach = scrapy.Field()
     SKU=scrapy.Field()
     The_Loai=scrapy.Field()
     Gia=scrapy.Field()
     Ten_NCC=scrapy.Field()
     Ten_NXB=scrapy.Field()
     Nam_XB=scrapy.Field()
     Ten_Tac_Gia=scrapy.Field()
     Trong_Luong=scrapy.Field()
     Kich_Thuoc=scrapy.Field()
     So_Trang=scrapy.Field()
     Hinh_Thuc=scrapy.Field()
     Mo_Ta=scrapy.Field()
