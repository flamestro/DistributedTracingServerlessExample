import io
import os

from minio import Minio
from minio.error import ResponseError

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id

try:
    from injectit import invoke_action, invoke_action_async
except ImportError:
    from src.utils.injectit import invoke_action, invoke_action_async

trace_context = TraceContext()
_DATA_STORE_ENDPOINT_ = '192.168.178.62'


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


def fetch_csv_file(csv_url):
    """
    fetches a csv file from a given url and returns it
    :param csv_url: a url to an file hosted on a minio server
    :return: returns a csv file as instance of CsvFile
    """
    splitted_csv_url = csv_url.split("/")
    filename = splitted_csv_url[-1]
    bucket = splitted_csv_url[-2]
    endpoint = splitted_csv_url[-3]
    with Span(span_name='fetch_csv_file', trace_context=trace_context):
        minio_client = Minio('{}'.format(endpoint),
                             access_key='AKIAIOSFODNN7EXAMPLE',
                             secret_key='wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY',
                             secure=False)
        file = minio_client.get_object(bucket, filename)
        with io.BytesIO(file.read()) as f:
            csv_file = CsvFile(file=f.read(), size=f.getbuffer().nbytes)
    return csv_file


def save_file_in_minio(csv_file, shop_key):
    """
    creates a file in the minio stores 'productstore' bucket
    :param shop_key: A string that is used to specify a shop
    :param csv_file: A object that is an instance of CsvFile
    :return:
    """
    with Span(span_name='save_file_in_minio', trace_context=trace_context):
        filename = "{}:{}".format(generate_id(), shop_key)
        minio_client = Minio('{}:9991'.format(_DATA_STORE_ENDPOINT_),
                             access_key='AKIAIOSFODNN7EXAMPLE',
                             secret_key='wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY',
                             secure=False)
        try:
            with io.BytesIO(csv_file.file) as file:
                minio_client.put_object('productstore', filename, file, csv_file.size, content_type='application/csv')
                print("published file: {}".format(filename))
                return filename
        except ResponseError as err:
            print(err)
            return "ERROR"


def call_fetch_product_image_in_batches(csv_lines, max_batch_amount, shop_key):
    with Span(trace_context=trace_context, span_name='call_fetch_product_image_in_batches'):
        first_line = csv_lines.pop(0)
        image_urls_index = first_line.split(',').index('IMAGEURLS')
        id_index = first_line.split(',').index('ID')

        image_urls = []
        for line in csv_lines:
            product_id = line.split(',')[id_index]
            extraced_image_url_list = line.split(',')[image_urls_index].replace('"', "").split('|')
            for image_url in extraced_image_url_list:
                image_urls.append(ImageUrl(image_url, product_id).to_dict())
        # create batches
        max_batch_size = int(len(image_urls) / max_batch_amount) + 1
        batches = [image_urls[x:x + max_batch_size] for x in range(0, len(image_urls), max_batch_size)]

        for x in range(0, max_batch_amount):
            print(invoke_action_async('fetchProductImages',
                                      os.environ.get('__OW_API_HOST',  "172.17.0.2:31001"),
                                      os.environ.get('__OW_API_KEY', '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'),
                                      data={'__OW_TRACE_ID': trace_context.trace_id,
                                            'imageUrls': batches[x],
                                            'shopKey': shop_key},
                                      ignore_certs=True))


def main(args):
    try:
        # Initialize trace context
        global trace_context
        trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                     service_name='fetchCSV',
                                     transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                     tracer_endpoint='192.168.178.62',
                                     parent_id=args.get('__PARENT_TRACE_ID', ''),
                                     action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
        # Initialize parameters
        csv_url = args.get("csvUrl", "http://{}:9990/productdata/test.csv".format(_DATA_STORE_ENDPOINT_))
        shop_key = args.get("shopKey", "771d87188d568ddd")
        # get csv file (CsvFile instance)
        file = fetch_csv_file(csv_url)
        # save csv file in minio and return filename
        minio_save_result = save_file_in_minio(file, shop_key)

        call_fetch_product_image_in_batches(file.csv_lines(), 4, shop_key)
        return {"message": str(minio_save_result), "__OW_TRACE_ID": trace_context.trace_id}
    except Exception as e:
        print(e)
    return {'error': "could not fetch data properly"}


if __name__ == "__main__":
    main({"csvUrl": "http://{}:9990/productdata/test.csv".format(_DATA_STORE_ENDPOINT_), "shopKey": "771d87188d568ddd"})
