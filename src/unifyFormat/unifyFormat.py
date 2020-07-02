import io
import os

from minio import Minio

from src.utils.structures import CsvFile

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id

try:
    from structures import CsvFile, ImageUrl
except ImportError:
    from src.utils.structures import CsvFile, ImageUrl


trace_context = TraceContext()
_DATA_STORE_ENDPOINT_ = '192.168.178.62:9991'


def get_csv_file(filename):
    """
    gets a csv file from a given url and returns it
    :return: returns a csv file as instance of CsvFile
    """
    filename = filename
    bucket = "productstore"
    endpoint = _DATA_STORE_ENDPOINT_
    with Span(span_name='fetch_csv_file', trace_context=trace_context):
        minio_client = Minio('{}'.format(endpoint),
                             access_key='AKIAIOSFODNN7EXAMPLE',
                             secret_key='wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY',
                             secure=False)
        file = minio_client.get_object(bucket, filename)
        with io.BytesIO(file.read()) as f:
            csv_file = CsvFile(file=f.read(), size=f.getbuffer().nbytes)
    return csv_file


def process_products(csv_lines):
    pass


def main(args):
    # Initialize trace context
    global trace_context
    trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                 service_name='unifyFormat',
                                 transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                 tracer_endpoint='192.168.178.62',
                                 parent_id=args.get('__PARENT_TRACE_ID', ''),
                                 action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
    # initialize parameters
    filename = args.get("filename")
    csv_file = get_csv_file(filename)
    products = process_products(csv_file.csv_lines())
    with Span(span_name='specify_result', trace_context=trace_context):
        result = {'traceId': trace_context.trace_id, 'message': filename}
    return result


if __name__ == '__main__':
    print(get_csv_file("27b9c2f59dbd0ef5:771d87188d568ddd").file)
