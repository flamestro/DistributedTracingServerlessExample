import csv
import io
import os
import random

from minio import Minio, ResponseError

from src.utils.structures import CsvFile

_DATA_STORE_ENDPOINT_ = '192.168.178.62'


def generate_external_id():
    return random.randint(1000, 10000000)


def generate_quantity():
    return random.randint(0, 500)


def generate_price():
    valid_chars = '1234567890'
    return ''.join((random.choice(valid_chars) for i in range(5)))


def generate_delivery_parameters():
    valid_texts = [{'DTEXT': 'Deliverable in 3-5 days!', 'DTIME': 4},
                   {'DTEXT': 'Deliverable in 1-3 days!', 'DTIME': 2},
                   {'DTEXT': 'Deliverable in 5-10 days!', 'DTIME': 7},
                   {'DTEXT': 'Deliverable in 10-20 days!', 'DTIME': 15}]
    return random.choice(valid_texts)


def generate_description():
    valid_texts = ['Super cool product']
    return random.choice(valid_texts)


def generate_image_urls():
    valid_texts = ['https://github.com/flamestro/PersonalPortfolio/blob/master/public/resources/opensenseTitlePage.png?raw=true',
                   'https://github.com/flamestro/PersonalPortfolio/blob/master/public/resources/slackLikeClonePage.png?raw=true',
                   'https://github.com/flamestro/PersonalPortfolio/blob/master/public/resources/portfolioPage.png?raw=true',
                   'https://github.com/flamestro/PersonalPortfolio/blob/master/public/resources/blockcertsTitlePage.png?raw=true',
                   'https://avatars0.githubusercontent.com/u/23012283?s=460&u=0976e85e757cf6588dbd076ff04a610753c648ce&v=4']
    image_string = ''
    image_amount = random.randint(1, 5)
    for x in range(0, image_amount):
        image_string += random.choice(valid_texts)
        if x < image_amount - 1:
            image_string += '|'

    return image_string


def generate_name():
    valid_texts = ['Super ', 'ultra ', 'toaster ', 'car ', 'time-machine ']
    return ''.join((random.choice(valid_texts) for i in range(5)))


def generate_csv(rows=1):
    with open('products.csv', 'w', newline='') as file:
        fieldnames = ["PNAME", "PDESCRIPTION", "PPRICE", "DTIME", "DTEXT", "QUANTITY", "ID", "IMAGEURLS"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        delivery_params = generate_delivery_parameters()
        writer.writeheader()
        for _ in range(0, rows):
            writer.writerow({'PNAME': generate_name(),
                             'PDESCRIPTION': generate_description(),
                             'PPRICE': generate_price(),
                             'DTIME': delivery_params['DTIME'],
                             'DTEXT': delivery_params['DTEXT'],
                             'QUANTITY': generate_quantity(),
                             'ID': generate_external_id(),
                             'IMAGEURLS': generate_image_urls()})
    with open('products.csv', 'rb') as file:
        with io.BytesIO(file.read()) as f:
            csv_file = CsvFile(file=f.read(), size=f.getbuffer().nbytes)
            save_file_in_minio(csv_file)
    os.remove('products.csv')


def save_file_in_minio(csv_file):
    """
    creates a file in the minio stores 'productstore' bucket
    :param shop_key: A string that is used to specify a shop
    :param csv_file: A object that is an instance of CsvFile
    :return:
    """
    filename = "products.csv"
    minio_client = Minio('{}:9990'.format(_DATA_STORE_ENDPOINT_),
                         access_key='AKIAIOSFODNN7EXAMPLE',
                         secret_key='wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY',
                         secure=False)
    try:
        with io.BytesIO(csv_file.file) as file:
            minio_client.put_object('productdata', filename, file, csv_file.size, content_type='application/csv')
            print("published file: {}".format(filename))
            return filename
    except ResponseError as err:
        print(err)
        return "ERROR"


if __name__ == '__main__':
    generate_csv(100)
