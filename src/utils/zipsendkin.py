import requests
import random
import datetime


def generate_epoch_timestamp():
    return int(datetime.datetime.now().timestamp() * 1000000)


def generate_id():
    valid_chars = 'abcdef1234567890'
    return ''.join((random.choice(valid_chars) for i in range(16)))


class TraceContext(object):
    def __init__(self, trace_id=None, service_name='action-name',
                 transaction_id='', tracer_endpoint='localhost', parent_id='', action_trace_id='', sampling_rate=1.0):
        if trace_id is None:
            sampled = random.randint(0, 100)
            if sampled <= int(sampling_rate * 100):
                self.trace_id = generate_id()
        else:
            self.trace_id = trace_id
        self.service_name = service_name
        self.transaction_id = transaction_id
        self.tracer_endpoint = tracer_endpoint
        self.parent_id = parent_id
        self.action_trace_id = action_trace_id if trace_id != action_trace_id else trace_id


class Message(object):
    def __init__(self, key='', value=''):
        self.key = key
        self.value = value

    def to_dict(self):
        return {"key": str(self.key), "value": str(self.value)}


class Span(object):
    def __init__(self, parent_id='', span_name='action-step', trace_context=TraceContext(), message=None):
        self.trace_context = trace_context
        self.id = generate_id()
        self.parent_id = parent_id if trace_context.parent_id == '' else trace_context.parent_id
        self.span_name = span_name
        self.timestamp = generate_epoch_timestamp()
        self.message = message

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.message = Message("error", "ErrorType: {}, ErrorMessage: {}".format(exc_type, exc_value))
        if self.trace_context.trace_id is not None:
            flush_span(self)


def flush_span(span=Span()):
    try:
        start = span.timestamp
        duration = generate_epoch_timestamp() - start

        payload = [
            {
                'id': span.id,
                'traceId': span.trace_context.trace_id,
                'parentId': span.parent_id,
                'name': span.span_name,
                'timestamp': start,
                'duration': duration,
                'tags': {
                    'http.method': 'GET',
                },
                "binaryAnnotations": [
                    {
                        "key": "transactionId",
                        "value": span.trace_context.transaction_id,
                        "endpoint": {
                            "serviceName": span.trace_context.service_name
                        }
                    },
                    {
                        "key": "rootTraceId",
                        "value": span.trace_context.action_trace_id
                    }
                ]
            }
        ]
        if span.message is not None:
            payload[0].get("binaryAnnotations").append(span.message.to_dict())
        requests.post(
            'http://{}:9411/api/v1/spans'.format(span.trace_context.tracer_endpoint),
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=0.00000001
        )
        print("Trace flushed")
    except Exception as e:
        print('Could not log trace {}'.format(e))
