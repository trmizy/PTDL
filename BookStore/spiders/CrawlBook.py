import scrapy
from BookStore.items import BookstoreItem

class CraigslistcrawlerSpider(scrapy.Spider):
    name = "CrawlBook"
    allowed_domains = ["nhanvan.vn"]
    
    def start_requests(self):
        yield scrapy.Request(url='https://www.nhanvan.vn/collections/thu-vien-sach-nhan-van?page=1', callback=self.parse)
    
    def parse(self, response):
        bookList = response.xpath('//div[@class="content-product-list row"]/div/div/div/a/@href').getall()
        for bookItem in bookList:
            item = BookstoreItem()
            request = scrapy.Request(url=response.urljoin(bookItem), callback=self.parseBookDetailPage)
            request.meta['databook'] = item
            yield request
        
        # Lấy đường dẫn trang tiếp theo trong phương thức parse
        next_page = response.xpath('//div[@id="pagination"]//a[@class="next "]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)  # Chuyển đổi relative URL sang absolute URL
            yield scrapy.Request(url=next_page_url, callback=self.parse)  
            
            
    def parseBookDetailPage(self, response):
        item = response.meta['databook']
        item['Ten_Sach'] = response.xpath('normalize-space(string(//div[@id="productDetail"]/h1))').get()
        item['SKU'] = response.xpath('normalize-space(string(//div[@id="productDetail"]/p[@class="sku"]))').get()
        item['The_Loai'] = response.xpath('normalize-space(string(//div[@id="productDetail"]/div[@class="product-info"]/p))').get()
        item['Gia'] = response.xpath('normalize-space(string(//div[@id="productDetail"]/div[@class="block-quantity-mb clearfix"]/div/span))').get()
        item['Ten_NCC'] = response.xpath('normalize-space(string(//td[text()="Tên nhà cung cấp"]/following-sibling::td/text()))').get()
        item['Ten_NXB'] = response.xpath('normalize-space(string(//td[text()="NXB"]/following-sibling::td/text()))').get()
        item['Nam_XB'] = response.xpath('normalize-space(string(//td[text()="Năm XB"]/following-sibling::td/text()))').get()
        item['Ten_Tac_Gia'] = response.xpath('normalize-space(string(//td[text()="Tác giả"]/following-sibling::td/text()))').get()
        item['Trong_Luong'] = response.xpath('normalize-space(string(//td[text()="Trọng lượng(gr)" or text()="Trọng lượng (gr)"]/following-sibling::td/text()))').get()
        item['Kich_Thuoc'] = response.xpath('normalize-space(string(//td[text()="Kích thước" or text()="Kích Thước Bao Bì"]/following-sibling::td/text()))').get()
        item['So_Trang'] = response.xpath('normalize-space(string(//td[text()="Số trang"]/following-sibling::td/text()))').get()
        item['Hinh_Thuc'] = response.xpath('normalize-space(string(//td[text()="Hình thức"]/following-sibling::td/text()))').get()
        # Lấy toàn bộ nội dung từ các thẻ p trong thẻ div có class 'item-content active'
        item['Mo_Ta'] = response.xpath('//div[contains(@class, "item-content active")]/p//text()').getall()
        item['Mo_Ta'] = ' '.join(item['Mo_Ta']).strip()  # Ghép các đoạn text lại thành một chuỗi
        yield item
        #Phuong thuc qua trang
        next_page = response.xpath('//div[@id="pagination"]//a[@class="next "]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)  # Chuyển đổi relative URL sang absolute URL
            yield scrapy.Request(url=next_page_url, callback=self.parse)