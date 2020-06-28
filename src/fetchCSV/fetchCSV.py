import io
import os

from urllib.request import urlopen
from minio import Minio
from minio.error import ResponseError

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id

trace_context = None


class CsvFile(object):
    def __init__(self, file, size):
        self.size = size
        self.file = file


def fetch_csv_file(url):
    """
    fetches a csv file from a given url and returns it
    :param url: a url to an file hosted on a ftp server
    :return: returns a csv file as instance of CsvFile
    """
    with Span(span_name='fetch_csv_file', trace_context=trace_context):
        file = urlopen(url)
        with io.BytesIO(file.read()) as f:
            csv_file = CsvFile(file=f.read(), size=f.getbuffer().nbytes)
    return csv_file


def save_file_in_minio(csv_file, shop_key):
    """

    :param csv_file:
    :param filename:
    :return:
    """
    with Span(span_name='save_file_in_minio', trace_context=trace_context):
        filename = "{}:{}".format(generate_id(), shop_key)
        minio_client = Minio('192.168.178.62:9991',
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


def main(args):
    # Initialize trace context
    global trace_context
    trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                 service_name='fetchCSV',
                                 transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                 tracer_endpoint='192.168.178.62',
                                 parent_id=args.get('__PARENT_TRACE_ID', ''),
                                 action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
    # Initialize parameters
    ftp_csv_url = args.get("ftpCsvUrl")
    shop_key = args.get("shopKey")

    # get csv file in CsvFile type
    file = fetch_csv_file(ftp_csv_url)
    # save csv file in minio and return filename
    minio_save_result = save_file_in_minio(file, shop_key)
    return {'message': minio_save_result}


main({"ftpCsvUrl": "ftp://localhost/test/test.csv",
      "shopKey": "771d87188d568ddd"})
