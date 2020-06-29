import io
import os
import requests
from minio import Minio
from minio.error import ResponseError

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id
try:
    from structures import File, ImageUrl
except ImportError:
    from src.utils.structures import File, ImageUrl

trace_context = TraceContext()
_DATA_STORE_ENDPOINT_ = '192.168.178.62'


def save_file_in_minio(img_file, shop_key, product_id, order):
    """
    creates a file in the minio stores 'productstore' bucket
    :param order: The order this image belongs to in a later preview
    :param product_id: The external product id
    :param shop_key: A string that is used to specify a shop
    :param img_file: A object that is an instance of File
    :return:
    """
    with Span(span_name='save_file_in_minio', trace_context=trace_context):
        filename = "{}:{}:{}.jpg".format(order, product_id, shop_key)
        minio_client = Minio('{}:9991'.format(_DATA_STORE_ENDPOINT_),
                             access_key='AKIAIOSFODNN7EXAMPLE',
                             secret_key='wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY',
                             secure=False)
        try:
            with io.BytesIO(img_file.file) as file:
                minio_client.put_object('productimages', filename, file, img_file.size)
                print("published file: {}".format(filename))
                return filename
        except ResponseError as err:
            print(err)
            return "ERROR"


def fetch_image_from_url(image_url):
    """
    fetches a image and returns it in bytes
    :param image_url: A url to an image
    :return: image data in bytes
    """
    with Span(span_name='fetch_image_from_url', trace_context=trace_context):
        img_data = requests.get(image_url).content
        with io.BytesIO(img_data) as f:
            img_file = File(file=f.read(), size=f.getbuffer().nbytes)
        return img_file


def main(args):
    # Initialize Trace Context
    global trace_context
    trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                 service_name='fetchProductImage',
                                 transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                 tracer_endpoint='192.168.178.62',
                                 parent_id=args.get('__PARENT_TRACE_ID', ''),
                                 action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
    # Initialize parameters
    image_urls = args.get("imageUrls", [])
    shop_key = args.get("shopKey", "771d87188d568ddd")
    for image_url in image_urls:
        image_file = fetch_image_from_url(image_url.get("imageUrl"))
        save_file_in_minio(image_file, shop_key, image_url.get("externalProductId"), image_url.get("order"))
    return {"message": "hi"}


if __name__ == "__main__":
    main({"imageUrls": [{
        "imageUrl": "https://avatars0.githubusercontent.com/u/23012283?s=460&u"
                    "=0976e85e757cf6588dbd076ff04a610753c648ce&v=4",
        "externalProductId": "213", "order": 0},
        {
            "imageUrl": "https://avatars0.githubusercontent.com/u/23012283?s=460&u"
                        "=0976e85e757cf6588dbd076ff04a610753c648ce&v=4",
            "externalProductId": "214", "order": 1}]})
