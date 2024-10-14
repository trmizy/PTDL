# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy
import json
import csv
import pymongo
from scrapy.exceptions import DropItem
import os
import re

class JsonDBBookStore:
    def __init__(self):
        self.first_item = True  # Theo dõi xem đây có phải là mục đầu tiên hay không

    def open_spider(self, spider):
        # Mở file và thêm dấu mở ngoặc vuông [
        with open('bookdata.json', 'w', encoding='utf-8') as file:
            file.write('[\n')

    def process_item(self, item, spider):
        with open('bookdata.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False)

            if self.first_item:
                file.write(line)  # Viết object đầu tiên mà không có dấu phẩy
                self.first_item = False
            else:
                file.write(',\n' + line)  # Viết dấu phẩy trước các object tiếp theo

        return item

    def close_spider(self, spider):
        # Khi quá trình cào kết thúc, thêm dấu đóng ngoặc vuông ]
        with open('bookdata.json', 'a', encoding='utf-8') as file:
            file.write('\n]')

class BookStorePipeline:
    def process_item(self, item, spider):
        # Xử lý nội dung của Mo_Ta, loại bỏ các ký tự không cần thiết
        if item.get('Mo_Ta'):
            # Xóa các ký tự xuống dòng và dư thừa khoảng trắng giữa các thẻ HTML
            item['Mo_Ta'] = re.sub(r'\s+', ' ', item['Mo_Ta']).strip()

        # Kiểm tra xem file CSV đã tồn tại chưa
        file_exists = os.path.isfile('bookdata.csv')

        # Ghi dữ liệu ra tệp CSV với dấu phân cách là dấu chấm phẩy
        if item:
            with open('bookdata.csv', 'a', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, delimiter=';')

                # Nếu file chưa tồn tại, thêm tiêu đề cột
                if not file_exists:
                    writer.writerow([
                        'Ten_Sach', 'SKU', 'The_Loai', 'Gia', 'Ten_NCC',
                        'Ten_NXB', 'Nam_XB', 'Ten_Tac_Gia', 'Trong_Luong',
                        'Kich_Thuoc', 'So_Trang', 'Hinh_Thuc', 'Mo_Ta'
                    ])

                # Ghi nội dung của item vào file
                writer.writerow([
                    item.get('Ten_Sach'),
                    item.get('SKU'),
                    item.get('The_Loai'),
                    item.get('Gia'),
                    item.get('Ten_NCC'),
                    item.get('Ten_NXB'),
                    item.get('Nam_XB'),
                    item.get('Ten_Tac_Gia'),
                    item.get('Trong_Luong'),
                    item.get('Kich_Thuoc'),
                    item.get('So_Trang'),
                    item.get('Hinh_Thuc'),
                    item.get('Mo_Ta')
                ])

            spider.log(f"Item written to CSV: {item}")
        else:
            spider.log(f"Empty item encountered: {item}")

        return item