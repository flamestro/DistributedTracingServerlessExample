import io
import os

from minio import Minio
from minio.error import ResponseError

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id

try:
    from injectit import invoke_action_async
except ImportError:
    from src.utils.injectit import invoke_action_async

try:
    from structures import CsvFile, ImageUrl
except ImportError:
    from src.utils.structures import CsvFile, ImageUrl

# global trace context object
trace_context = TraceContext()

_EXTERNAL_DATA_STORE_ENDPOINT_ = '192.168.178.62:9990'
_INTERNAL_DATA_STORE_ENDPOINT_ = '192.168.178.62:9991'
# MinIO ACCESS DATA
_MINIO_ACCESS_KEY_ = "AKIAIOSFODNN7EXAMPLE"
_MINIO_SECRET_KEY_ = "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"
# Endpoint and auth OpenWhisk (Important to test and debug locally, this should not be done on production environments)
_OPENWHISK_HOST_ENDPOINT_ = '172.17.0.2:31001'
_OPENWHISK_KEY_ = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
# ZipKin Collector Endpoint
_TRACER_ENDPOINT_ = '192.168.178.62'


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
                             access_key=_MINIO_ACCESS_KEY_,
                             secret_key=_MINIO_SECRET_KEY_,
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
        minio_client = Minio('{}'.format(_INTERNAL_DATA_STORE_ENDPOINT_),
                             access_key=_MINIO_ACCESS_KEY_,
                             secret_key=_MINIO_SECRET_KEY_,
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
    """
    Processes csv lines and extracts instances of the Class ImageUrl to a list of dicts.
    Passes this list to asynchronously called fetchProductImages actions
    :param csv_lines: A list of strings that represent all the lines of the handled csv file
    :param max_batch_amount: The maximum amount of batches that should be handled by asynchronously called
    fetchProductImages actions
    :param shop_key: A string that represent a shop
    :return:
    """
    with Span(trace_context=trace_context, span_name='call_fetch_product_image_in_batches'):
        first_line = csv_lines.pop(0)
        image_urls_index = first_line.split(',').index('IMAGEURLS')
        id_index = first_line.split(',').index('ID')

        image_urls = []
        # create ImageUrl instances
        for line in csv_lines:
            product_id = line.split(',')[id_index]
            extraced_image_url_list = line.split(',')[image_urls_index].split('|')
            count = 0
            for image_url in extraced_image_url_list:
                image_urls.append(ImageUrl(image_url, product_id, count).to_dict())
                count += 1
        # create batches
        max_batch_size = int(len(image_urls) / max_batch_amount) + 1
        batches = [image_urls[x:x + max_batch_size] for x in range(0, len(image_urls), max_batch_size)]

        # call fetchProductImages
        for x in range(0, max_batch_amount):
            print(batches[x])
            print("activationId:{}".format(invoke_action_async('fetchProductImages',
                                                               os.environ.get('__OW_API_HOST', _OPENWHISK_HOST_ENDPOINT_),
                                                               os.environ.get('__OW_API_KEY', _OPENWHISK_KEY_),
                                                               data={'__OW_TRACE_ID': trace_context.trace_id,
                                                                     'imageUrls': batches[x],
                                                                     'shopKey': shop_key},
                                                               ignore_certs=True)))


def main(args):
    try:
        # Initialize trace context
        global trace_context
        trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                     service_name='fetchCSV',
                                     transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                     tracer_endpoint=_TRACER_ENDPOINT_,
                                     parent_id=args.get('__PARENT_TRACE_ID', ''),
                                     action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
        # Initialize parameters
        csv_url = args.get("csvUrl", "http://{}/productdata/products.csv".format(_EXTERNAL_DATA_STORE_ENDPOINT_))
        shop_key = args.get("shopKey", "771d87188d568ddd")
        # get csv file (CsvFile instance)
        file = fetch_csv_file(csv_url)
        # save csv file in minio and return filename
        minio_save_result = save_file_in_minio(file, shop_key)

        call_fetch_product_image_in_batches(file.csv_lines(), 2, shop_key)
        print(minio_save_result)
        return {"filename": str(minio_save_result), "shopKey": shop_key, "__OW_TRACE_ID": trace_context.trace_id}
    except Exception as e:
        print(e)
        return {'error': "could not fetch data properly {}".format(e)}


if __name__ == "__main__":
    main({"csvUrl": "http://{}/productdata/products.csv".format(_EXTERNAL_DATA_STORE_ENDPOINT_),
          "shopKey": "771d87188d568ddd"})
