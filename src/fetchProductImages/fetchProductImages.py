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
try:
    from injectit import invoke_action
except ImportError:
    from src.utils.injectit import invoke_action

trace_context = TraceContext()
_WRAPPER_PARENT_SPAN_ID_ = ""
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
    with Span(span_name='save_file_in_minio', trace_context=trace_context, parent_id=_WRAPPER_PARENT_SPAN_ID_):
        filename = "{}:{}:{}".format(order, product_id, shop_key)
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
    with Span(span_name='fetch_image_from_url', trace_context=trace_context, parent_id=_WRAPPER_PARENT_SPAN_ID_):
        img_data = requests.get(image_url).content
        with io.BytesIO(img_data) as f:
            img_file = File(file=f.read(), size=f.getbuffer().nbytes)
        return img_file


def call_thumbnail_generator(filename):
    with Span(span_name='call_thumbnail_generator', trace_context=trace_context, parent_id=_WRAPPER_PARENT_SPAN_ID_):
        invoke_action('thumbnailGenerator',
                      os.environ.get('__OW_API_HOST', "172.17.0.2:31001"),
                      os.environ.get('__OW_API_KEY',
                                     '23bc46b1-71f6-4ed5-8c54-816aa4f8c502'
                                     ':123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'),
                      data={'__OW_TRACE_ID': trace_context.trace_id,
                            '__PARENT_TRACE_ID': _WRAPPER_PARENT_SPAN_ID_,
                            'imageName': filename},
                      ignore_certs=True)


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
    with Span(span_name='fetch_images', trace_context=trace_context) as wrapper_context:
        global _WRAPPER_PARENT_SPAN_ID_
        _WRAPPER_PARENT_SPAN_ID_ = wrapper_context.id
        for image_url in image_urls:
            image_file = fetch_image_from_url(image_url.get("imageUrl"))
            filename = save_file_in_minio(image_file, shop_key, image_url.get("externalProductId"),
                                          image_url.get("order"))
            if int(image_url.get("order")) == 0:
                call_thumbnail_generator(filename)
    return {"message": "hi"}


if __name__ == "__main__":
    main({"imageUrls": [{
        "imageUrl": "https://avatars0.githubusercontent.com/u/23012283?s=460&u"
                    "=0976e85e757cf6588dbd076ff04a610753c648ce&v=4",
        "externalProductId": "213",
        "order": 0}, {
        "imageUrl": "https://avatars0.githubusercontent.com/u/23012283?s=460&u"
                    "=0976e85e757cf6588dbd076ff04a610753c648ce&v=4",
        "externalProductId": "214",
        "order": 1}]})
