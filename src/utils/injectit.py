import requests

local_url = 'https://{0}/api/v1/namespaces/_/actions/{1}'
web_url = 'https://{0}/api/v1/web/_/default/{1}'


def invoke_action(action_name, api_host, api_key, data=None, ignore_certs=True, web=False):
    """
    usage:
    invoke_action('hello-world',
                os.environ['__OW_API_HOST'],
                os.environ['__OW_API_KEY'])
    """
    if data is None:
        data = {}
    api_host = api_host.replace('https://', '')
    run_url = web_url if web else local_url
    api_user = api_key.split(':')[0]
    api_password = api_key.split(':')[1]
    response = requests.post(run_url.format(api_host, action_name),
                             auth=(api_user, api_password),
                             params={"blocking": "true", 'response': 'true'},
                             json=data,
                             verify=not ignore_certs).json()
    return response['response']['result']


def invoke_action_async(action_name, api_host, api_key, ignore_certs=True, web=False):
    """
    usage:
    invoke_action_async('hello-world',
                os.environ['__OW_API_HOST'],
                os.environ['__OW_API_KEY'])
    """
    api_host = api_host.replace('https://', '')
    run_url = web_url if web else local_url
    api_user = api_key.split(':')[0]
    api_password = api_key.split(':')[1]
    response = requests.post(run_url.format(api_host, action_name),
                             auth=(api_user, api_password),
                             params={"blocking": "false", 'response': 'false'},
                             verify=not ignore_certs).json()
    return response['response']['result']
