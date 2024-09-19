from datetime import datetime
from typing import Iterable, Any

import scrapy
from scrapy import Request
from scrapy.http import Response


class FixpriceSpider(scrapy.Spider):
    name = 'fix_price'
    api_url = 'https://api.fix-price.com/buyer/v1/product/in/'

    start_urls = [
        'https://fix-price.com/catalog/dlya-doma/tovary-dlya-uborki',
        'https://fix-price.com/catalog/kantstovary/kantselyarskie-prinadlezhnosti',
        'https://fix-price.com/catalog/avto-moto-velo'
    ]

    headers = {
        'Sec-Ch-Ua': '"Not;A=Brand";v="24", "Chromium";v="128"',
        'X-Language': 'ru',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Sec-Ch-Ua-Mobile': '?0',
        'X-Key': 'fbd3342530f997690891b9dfb24e3a6b',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/128.0.6613.120 Safari/537.36',
        'X-City': '55'  # Екатеринбург
    }

    def get_category_page_url(self, category, page):
        return f'{self.api_url}{category}?page={page}&limit=24&sort=sold'

    def start_requests(self) -> Iterable[Request]:
        page = 1
        for url in self.start_urls:
            category = url.split('/catalog/')[-1]
            api_full_url = self.get_category_page_url(category, page)
            yield scrapy.Request(
                url=api_full_url,
                headers=self.headers,
                method='POST',
                callback=self.parse,
                meta={'category': category, 'page': page},
                dont_filter=True,
            )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        products = response.json()
        for product in products:
            product_url = f'https://api.fix-price.com/buyer/v1/product/{product["url"]}'

            yield scrapy.Request(url=product_url, headers=self.headers, callback=self.parse_details)

        if len(products) == 24:  # 24 товаров на 1 странице
            next_page = response.meta['page'] + 1
            category = response.meta['category']
            api_full_url = self.get_category_page_url(category, next_page)

            yield scrapy.Request(
                url=api_full_url,
                headers=self.headers,
                method='POST',
                callback=self.parse,
                meta={'category': category, 'page': next_page},
                dont_filter=True
            )

    def parse_details(self, response):
        product = response.json()

        rpc = product['id']
        product_url = f'https://fix-price.com/catalog/{product["url"]}'
        title = product['title']
        brand = product['brand']['title'] if product.get('brand') else ''

        product_variant = product['variants'][0]

        price_data = {}
        if product_variant['price']:
            price_data['current'] = product_variant['price']

        if product_variant['fixPrice']:
            price_data['original'] = product_variant['fixPrice']

        if price_data['current'] < price_data['original']:
            discount_percentage = round(100 * (1 - price_data['current'] / price_data['original']), 2)
            price_data['sale_tag'] = f'Скидка {discount_percentage}%'

        stock = {}
        stock['in_stock'] = bool(product_variant['count'])
        stock['count'] = product_variant['count'] if stock['in_stock'] else 0

        assets = {'main_image': '', 'set_images': []}
        images = product.get('images')
        if images:
            assets['main_image'] = images[0]['src']
            for image in images:
                assets['set_images'].append(image['src'])

        if product['video']:
            assets['video'] = list(product['video'])

        metadata = {}
        metadata['__description'] = product['description']

        properties = product.get('properties')
        if properties:
            metadata['СТРАНА ПРОИЗВОДСТВА'] = product['properties'][0]['value']

        if product['variants']:
            metadata['ШИРИНА'] = product_variant['width']
            metadata['ВЫСОТА'] = product_variant['height']
            metadata['ДЛИННА'] = product_variant['length']
            metadata['ВЕС'] = product_variant['weight']
            metadata['ШТРИХ-КОД'] = product_variant['barcode']

        variants = len(product['variants'])

        yield {
            'timestamp': datetime.now(),
            'RPC': rpc,
            'url': product_url,
            'title': title,
            'brand': brand,
            'price_data': price_data,
            'stock': stock,
            'assets': assets,
            'metadata': metadata,
            'variants': variants
        }
