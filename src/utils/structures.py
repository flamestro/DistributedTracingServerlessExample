class CsvFile(object):
    def __init__(self, file, size):
        self.size = size
        self.file = file

    def to_string(self):
        return str(self.file)[2:-1]

    def csv_lines(self):
        csv_lines_list = self.to_string().split("\\n")
        csv_lines_list = list(map(lambda x: x.replace("\\r", ""), csv_lines_list))
        csv_lines_list.remove("")
        return csv_lines_list


class File(object):
    def __init__(self, file, size):
        self.size = size
        self.file = file

    def to_string(self):
        return str(self.file)[2:-1]


class ImageUrl(object):
    def __init__(self, image_url, product_id, order):
        self.image_url = image_url
        self.product_id = product_id
        self.order = order

    def to_dict(self):
        return {'imageUrl': self.image_url,
                'externalProductId': self.product_id,
                'order': self.order}


class Product(object):
    def __init__(self, thumbnail, name, product_images, quantity, price, delivery_text, delivery_time,
                 description, external_id, shop_key):
        self.thumbnail = thumbnail
        self.name = name
        # A list of product image urls represented as strings
        self.product_images = product_images
        self.quantity = quantity
        self.price = price
        self.delivery_text = delivery_text
        self.delivery_time = delivery_time
        self.description = description
        self.external_id = external_id
        self.shop_key = shop_key

    def to_dict(self):
        return {'thumbnail': self.thumbnail,
                'name': self.name,
                'productImages': self.product_images,
                'quantity': self.quantity,
                'price': self.price,
                'deliveryText': self.delivery_text,
                'deliveryTime': self.delivery_time,
                'description': self.description,
                'externalId': self.external_id,
                'shopKey': self.shop_key}
