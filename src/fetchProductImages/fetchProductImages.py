import os

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id


def main(args):
    # Initialize Trace Context
    trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                 service_name='fetchProductImage',
                                 transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                 tracer_endpoint='192.168.178.62',
                                 parent_id=args.get('__PARENT_TRACE_ID', ''),
                                 action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
    # Initialize parameters
    image_urls = args.get("imageUrls", [])
    shop_key = args.get("shopKey", "771d87188d568ddd")

    with Span(span_name='specify_result', trace_context=trace_context):
        result = {'message': 'Hello World!'}
    return result
