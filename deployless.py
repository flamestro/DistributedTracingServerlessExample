#!/usr/bin/env python3

import os
import shutil
import sys
import time
from pathlib import Path
from zipfile import ZipFile

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from yaml import load
import base64

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


# OpenWhisk Url's
local_url = 'https://{0}/api/v1/namespaces/_/actions/{1}'
web_url = 'https://{0}/api/v1/web/_/default/{1}'


# Read User Config
with open('../deployless.yaml', "r") as f:
    raw = f.read()
    deploy_config = load(raw, Loader=Loader)
    actions = deploy_config['actions']
    provider = deploy_config['provider']
    username = provider['auth'].split(':')[0]
    password = provider['auth'].split(':')[1]
    api_host = provider['api-host']


def list_to_line(list_param):
    line = ''
    for elem in list_param:
        line += ' ' + elem
    return line


def openwhisk_deployment():
    Path("build").mkdir(parents=True, exist_ok=True)
    # iterate over specified actions
    for action_name in actions.keys():
        action_config = actions[action_name]
        action_path = action_config['main']
        dependencies = ['__main__.py']
        attributes = []
        web = False
        print("Started deployment of " + action_name)

        # -------------- READ ATTRIBUTES --------------
        # Add Ignore Certs Attribute if specified in provider
        if 'ignore-certs' in provider.keys():
            if provider['ignore-certs'] is True:
                attributes.append('-i')

        # Check if Action is Web Action
        if 'web' in action_config.keys():
            if action_config['web'] is True:
                web = True

        # Load Dependencies
        if 'dependencies' in action_config.keys():
            for dependency in action_config['dependencies']:
                dependencies.append(dependency)

        # If Action has Custom Requirements Create a Virtual Environment
        if 'requirements' in action_config.keys():
            dependencies.append('build/virtualenv')
            action_requirements = action_config['requirements']

            shutil.copyfile(action_requirements, 'build/requirements.txt')
            os.system(
                'cd build && docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c \
                "cd tmp && \
                 virtualenv virtualenv && \
                 source virtualenv/bin/activate && \
                 pip install -r requirements.txt" > /dev/null'.format(action_requirements))
            # Needs some time so that deployment does not need to be triggered 2 times
            time.sleep(5)
            os.remove('build/requirements.txt')
        print('Attributes are: ' + str(attributes))
        print('Dependencies are: ' + str(dependencies))

        # Deployment of Action
        shutil.copyfile(action_path, '../__main__.py')
        zipped_code = ZipFile('build/{}.zip'.format(action_name), 'w')
        for dependency in dependencies:
            zipped_code.write(dependency)
        zipped_code.close()
        with open('build/{}.zip'.format(action_name), 'rb') as zipped_code:
            bytes_zipped_code = zipped_code.read()
        encoded_zipped_code = base64.b64encode(bytes_zipped_code)
        response = requests.put(local_url.format(api_host, action_name),
                                auth=(username, password),
                                params={"overwrite": "true"},
                                verify=not ('-i' in attributes),
                                headers={'Content-type': 'application/json'},
                                json={
                                    'name': action_name,
                                    'namespace': '_',
                                    'exec': {
                                        'kind': action_config['kind'],
                                        'code': str(encoded_zipped_code)[2:-1],
                                        'binary': True
                                    },
                                    'annotations': [
                                        {"key": "web-export", "value": web},
                                        {"key": "raw-http", "value": False},
                                        {"key": "final", "value": True}
                                    ]
                                })
        if response.status_code == 200:
            print('Deployed : {} \n'.format(action_name))
        else:
            print(response)
        os.remove('../__main__.py')
        os.remove('build/{0}.zip'.format(action_name))


def openwhisk_clear():
    attributes = []
    if 'ignore-certs' in provider.keys():
        if provider['ignore-certs'] is True:
            attributes.append('-i')
    for action_name in actions.keys():
        response = requests.delete(local_url.format(api_host, action_name),
                                   auth=(username, password),
                                   verify=not ('-i' in attributes))
        print('DELETED - action: {} status: {}'.format(action_name, response.status_code))


def openwhisk_run(action_name):
    run_url = local_url
    attributes = ['-b', '-r']
    if 'ignore-certs' in provider.keys():
        if provider['ignore-certs'] is True:
            attributes.append('-i')
    # Check if Action is Web Action
    if 'web' in actions[action_name].keys():
        if actions[action_name]['web'] is True:
            run_url = web_url
    response = requests.post(run_url.format(api_host, action_name),
                             auth=(username, password),
                             params={"blocking": "true", 'response': 'true'},
                             verify=not ('-i' in attributes))
    print(response.json()['response']['result'])


def main():
    # OpenWhisk specific behaviour
    if deploy_config['provider']['platform'] == 'openwhisk':
        arguments = sys.argv[1:]
        if '--clear' in arguments:
            openwhisk_clear()
        elif '--run' in arguments:
            openwhisk_run(arguments[arguments.index('--run') + 1])
        else:
            openwhisk_deployment()


if __name__ == '__main__':
    try:
        main()
    except:
        print("Could not connect to platform")
