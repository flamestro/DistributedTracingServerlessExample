import io
import os

from minio import Minio

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id

try:
    from structures import CsvFile, ImageUrl, Product
except ImportError:
    from src.utils.structures import CsvFile, ImageUrl, Product

try:
    from injectit import invoke_action
except ImportError:
    from src.utils.injectit import invoke_action

# global trace context object
trace_context = TraceContext()

_INTERNAL_DATA_STORE_ENDPOINT_ = '192.168.178.62:9991'
# MinIO ACCESS DATA
_MINIO_ACCESS_KEY_ = "AKIAIOSFODNN7EXAMPLE"
_MINIO_SECRET_KEY_ = "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"
# Endpoint and auth OpenWhisk (Important to test and debug locally, this should not be done on production environments)
_OPENWHISK_HOST_ENDPOINT_ = '172.17.0.2:31001'
_OPENWHISK_KEY_ = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
# ZipKin Collector Endpoint
_TRACER_ENDPOINT_ = '192.168.178.62'


def get_csv_file(filename):
    """
    gets a csv file from a given url and returns it
    :return: returns a csv file as instance of CsvFile
    """
    with Span(span_name='fetch_csv_file', trace_context=trace_context):
        filename = filename
        bucket = "productstore"
        endpoint = _INTERNAL_DATA_STORE_ENDPOINT_
        minio_client = Minio('{}'.format(endpoint),
                             access_key=_MINIO_ACCESS_KEY_,
                             secret_key=_MINIO_SECRET_KEY_,
                             secure=False)
        file = minio_client.get_object(bucket, filename)
        with io.BytesIO(file.read()) as f:
            csv_file = CsvFile(file=f.read(), size=f.getbuffer().nbytes)
    return csv_file


def get_image_count(image_urls):
    """
    :param image_urls: A string that contains image urls separated by the | symbol
    :return: the amout of image urls in the string
    """
    return image_urls.count("|") + 1


def process_products(csv_lines, shop_key):
    """
    Processed products from csv files to a internal presentation as dicts
    :param shop_key: A string that represent a shop
    :param csv_lines: A list of strings that represent the lines in a csv file
    :return: a status that tells if the extraction of products failed or not, and if not, it contains also the products
    in a list of dicts
    """
    with Span(span_name='process_products', trace_context=trace_context):

        first_line = csv_lines.pop(0).split(',')
        products = []

        # get indexes of all fields
        try:
            product_name_index = first_line.index("PNAME")
            product_description_index = first_line.index("PDESCRIPTION")
            product_price_index = first_line.index("PPRICE")
            delivery_time_index = first_line.index("DTIME")
            delivery_text_index = first_line.index("DTEXT")
            quantity_index = first_line.index("QUANTITY")
            id_index = first_line.index("ID")
            image_urls_index = first_line.index("IMAGEURLS")
        except:
            print('could not read relevant field')
            return {'status': 'failed'}

        # process lines
        for line in csv_lines:
            line = line.split(',')
            image_url_count = get_image_count(line[image_urls_index])
            product_id = line[id_index]
            quantity = line[quantity_index]
            delivery_text = line[delivery_text_index]
            delivery_time = line[delivery_time_index]
            product_price = line[product_price_index]
            product_description = line[product_description_index]
            product_name = line[product_name_index]

            # check if values are mappable to the expected datatypes
            try:
                int(product_id)
            except:
                print('id is not an integer skipping line')
                continue
            try:
                int(quantity)
            except:
                print('quantity is not an integer skipping line')
                continue
            try:
                int(quantity)
            except:
                print('quantity is not an integer skipping line')
                continue
            try:
                str(delivery_text)
            except:
                print('delivery text is not mappable to a string skipping line')
                continue
            try:
                int(delivery_time)
            except:
                print('delivery time is not an integer skipping line')
                continue
            try:
                int(product_price)
            except:
                print('product price is not an integer skipping line')
                continue
            try:
                str(product_description)
            except:
                print('product description is not mappable to a string skipping line')
                continue
            try:
                str(product_name)
            except:
                print('product name is not mappable to a string skipping line')
                continue

            # create image filenames
            images = []
            for i in range(0, image_url_count):
                images.append("{}:{}:{}".format(i, product_id, shop_key))

            # The thumbnail would contain another value if the thumbnail generator would be fully implemented
            thumbnail = "{}:{}:{}".format(0, product_id, shop_key)
            products.append(Product(thumbnail=thumbnail, name=product_name, product_images=images,
                                    external_id=product_id, quantity=quantity, description=product_description,
                                    delivery_time=delivery_time, delivery_text=delivery_text,
                                    price=product_price, shop_key=shop_key).to_dict())
        return {"status": "success", "products": products}


def main(args):
    try:
        # Initialize trace context
        global trace_context
        trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                     service_name='unifyFormat',
                                     transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                     tracer_endpoint=_TRACER_ENDPOINT_,
                                     parent_id=args.get('__PARENT_TRACE_ID', ''),
                                     action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
        # initialize parameters
        filename = str(args.get("filename"))
        shop_key = str(args.get("shopKey"))

        csv_file = get_csv_file(filename)
        products = process_products(csv_file.csv_lines(), shop_key)
        # TODO: Logic for failures (check status)
        with Span(span_name='invoke_productsApi', trace_context=trace_context):
            invoke_action("productsApi",
                          os.environ.get('__OW_API_HOST', _OPENWHISK_HOST_ENDPOINT_),
                          os.environ.get('__OW_API_KEY', _OPENWHISK_KEY_),
                          data={'__OW_TRACE_ID': trace_context.trace_id,
                                'products': products},
                          ignore_certs=True)
        return {'__OW_TRACE_ID': trace_context.trace_id, 'products': products.get('products', [])}
    except Exception as e:
        return {"error": "{}".format(e)}


if __name__ == '__main__':
    """
    Used to locally run and debug this action -> Not executed by when called by OpenWhisk
    """
    main({
        "filename": "48e5b5a8f8f1e68c:771d87188d568ddd",
        "shopKey": "771d87188d568ddd"
    })
