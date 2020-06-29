class CsvFile(object):
    def __init__(self, file, size):
        self.size = size
        self.file = file

    def to_string(self):
        return str(self.file)[2:-1]

    def csv_lines(self):
        csv_lines_list = self.to_string().replace("\\n", "").replace("\\r", "").split(";")
        csv_lines_list.remove("")
        return csv_lines_list


class ImageUrl(object):
    def __init__(self, image_url, product_id):
        self.image_url = image_url
        self.product_id = product_id

    def to_dict(self):
        return {'imageUrl': self.image_url,
                'externalProductId': self.product_id}
