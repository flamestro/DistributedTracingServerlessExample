import io
import os
import requests
from minio import Minio
from minio.error import ResponseError

try:
    from zipsendkin import Span, TraceContext, generate_id, Message
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id, Message
try:
    from structures import File, ImageUrl
except ImportError:
    from src.utils.structures import File, ImageUrl
try:
    from injectit import invoke_action
except ImportError:
    from src.utils.injectit import invoke_action

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
_WRAPPER_PARENT_SPAN_ID_ = ""


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
        minio_client = Minio('{}'.format(_INTERNAL_DATA_STORE_ENDPOINT_),
                             access_key=_MINIO_ACCESS_KEY_,
                             secret_key=_MINIO_SECRET_KEY_,
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


def call_thumbnail_generator(filenames):
    """
    Calls the thumbnail generator for batches of filenames that qualify as thumbnail
    :param filenames: a list of filenames inside the productimages bucket
    :return: void
    """
    with Span(span_name='call_thumbnail_generator', trace_context=trace_context, parent_id=_WRAPPER_PARENT_SPAN_ID_,
              message=Message(key="Filenames", value=filenames)) as parent:
        invoke_action('thumbnailGenerator',
                      os.environ.get('__OW_API_HOST', _OPENWHISK_HOST_ENDPOINT_),
                      os.environ.get('__OW_API_KEY', _OPENWHISK_KEY_),
                      data={'__OW_TRACE_ID': trace_context.trace_id,
                            '__PARENT_TRACE_ID': parent.id,
                            'imageNames': filenames},
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
        thumbnails = []
        for image_url in image_urls:
            with Span(span_name='fetch_image', trace_context=trace_context, parent_id=wrapper_context.id) as wrapper_image_context:
                global _WRAPPER_PARENT_SPAN_ID_
                _WRAPPER_PARENT_SPAN_ID_ = wrapper_image_context.id
                image_file = fetch_image_from_url(image_url.get("imageUrl"))
                filename = save_file_in_minio(image_file, shop_key, image_url.get("externalProductId"),
                                              image_url.get("order"))
                if int(image_url.get("order")) == 0:
                    thumbnails.append(filename)
        call_thumbnail_generator(thumbnails)
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
