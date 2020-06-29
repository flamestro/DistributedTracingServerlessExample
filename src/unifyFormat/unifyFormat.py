import os

try:
    from zipsendkin import Span, TraceContext, generate_id
except ImportError:
    from src.utils.zipsendkin import Span, TraceContext, generate_id


def main(args):
    # initialize trace context
    trace_context = TraceContext(trace_id=args.get('__OW_TRACE_ID', os.environ.get('__OW_TRACE_ID', generate_id())),
                                 service_name='unifyFormat',
                                 transaction_id=os.environ.get('__OW_TRANSACTION_ID', ''),
                                 tracer_endpoint='192.168.178.62',
                                 parent_id=args.get('__PARENT_TRACE_ID', ''),
                                 action_trace_id=os.environ.get('__OW_TRACE_ID', ''))
    # initialize parameters
    filename = args.get("filename")

    with Span(span_name='specify_result', trace_context=trace_context):
        result = {'traceId': trace_context.trace_id, 'message': filename}
    return result
