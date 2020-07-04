#!/usr/bin/env python3
import os
import shutil
import sys
import time
from pathlib import Path

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
log_url = 'https://{0}/api/v1/namespaces/_/activations/{1}/logs'
activations_url = 'https://{0}/api/v1/namespaces/_/activations'

# Read User Config
with open('deployless.yaml', "r") as f:
    raw = f.read()
    deploy_config = load(raw, Loader=Loader)
    actions = deploy_config['actions']
    sequences = deploy_config.get('sequences', {})
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
        Path("build/{}".format(action_name)).mkdir(parents=True, exist_ok=True)
        action_config = actions[action_name]
        action_path = action_config['main']
        dependencies = ['build/__main__.py']
        attributes = []
        web = False

        # Action Limits
        # timeout in milliseconds (default: 60000)
        timeout = 60000
        # memory in megabytes (default: 256)
        memory = 256
        # log size in megabytes (default: 10)
        logs = 10
        # number of concurrent activations allowed (default: 1)
        concurrency = 1

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

        # Check Action Limits
        if 'timeout' in action_config.keys():
            try:
                int(action_config['timeout'])
                timeout = action_config['timeout']
            except:
                print("Timeout config for {} is not an integer, falling back to default".format(action_name))

        if 'memory' in action_config.keys():
            try:
                int(action_config['memory'])
                memory = action_config['memory']
            except:
                print("Memory config for {} is not an integer, falling back to default".format(action_name))

        if 'logs' in action_config.keys():
            try:
                int(action_config['logs'])
                logs = action_config['logs']
            except:
                print("Logs config for {} is not an integer, falling back to default".format(action_name))

        if 'concurrency' in action_config.keys():
            try:
                int(action_config['concurrency'])
                concurrency = action_config['concurrency']
            except:
                print("Concurrency config for {} is not an integer, falling back to default".format(action_name))

        # If Action has Custom Requirements Create a Virtual Environment
        if 'requirements' in action_config.keys():
            dependencies.append("build/{}/virtualenv".format(action_name))
            action_requirements = action_config['requirements']

            shutil.copyfile(action_requirements, 'build/{}/requirements.txt'.format(action_name))
            os.system(
                'docker run --rm -v "$PWD/build/{}/:/tmp" '
                '--user $(id -u):$(id -g) '
                'openwhisk/python3action bash -c \
                "cd tmp && \
                 virtualenv virtualenv && \
                 source virtualenv/bin/activate && \
                 pip3 install -r requirements.txt" > /dev/null'.format(action_name))
            # Needs some time so that deployment does not need to be triggered 2 times
            time.sleep(5)
            os.remove('build/{}/requirements.txt'.format(action_name))
        print('Attributes are: ' + str(attributes))
        print('Dependencies are: ' + str(dependencies))

        # Deployment of Action
        shutil.copyfile(action_path, 'build/{}/__main__.py'.format(action_name))
        for dependency in dependencies:
            if dependency != dependencies[0] and dependency != "build/{}/virtualenv".format(action_name):
                shutil.copyfile(dependency, 'build/{}/{}'.format(action_name, dependency.split('/')[-1]))
        shutil.make_archive('build/{}'.format(action_name), 'zip', 'build/{}/'.format(action_name))
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
                                    ],
                                    "limits": {
                                        "timeout": timeout,
                                        "memory": memory,
                                        "logs": logs,
                                        "concurrency": concurrency
                                    }
                                })
        if response.status_code == 200:
            print('Deployed : {} \n'.format(action_name))
        else:
            print(response.json())
        os.remove('build/{0}.zip'.format(action_name))
        shutil.rmtree("build/{}".format(action_name))

    for sequence_name in sequences.keys():
        attributes = []
        sequence_config = sequences[sequence_name]
        web = False
        print("Started deployment of Sequence: " + sequence_name)

        # Sequence Limits
        # timeout in milliseconds (default: 60000)
        timeout = 60000
        # memory in megabytes (default: 256)
        memory = 256
        # log size in megabytes (default: 10)
        logs = 10
        # number of concurrent activations allowed (default: 1)
        concurrency = 1

        # -------------- READ ATTRIBUTES --------------
        # Add Ignore Certs Attribute if specified in provider
        if 'ignore-certs' in provider.keys():
            if provider['ignore-certs'] is True:
                attributes.append('-i')

        # Check if Action is Web Action
        if 'web' in sequence_config.keys():
            if sequence_config['web'] is True:
                web = True

        # Check Sequence Limits
        if 'timeout' in sequence_config.keys():
            try:
                int(sequence_config['timeout'])
                timeout = sequence_config['timeout']
            except:
                print("Timeout config for {} is not an integer, falling back to default".format(action_name))

        if 'memory' in sequence_config.keys():
            try:
                int(sequence_config['memory'])
                memory = sequence_config['memory']
            except:
                print("Memory config for {} is not an integer, falling back to default".format(action_name))

        if 'logs' in sequence_config.keys():
            try:
                int(sequence_config['logs'])
                logs = sequence_config['logs']
            except:
                print("Logs config for {} is not an integer, falling back to default".format(action_name))

        if 'concurrency' in sequence_config.keys():
            try:
                int(sequence_config['concurrency'])
                concurrency = sequence_config['concurrency']
            except:
                print(
                    "Concurrency config for {} is not an integer, falling back to default".format(action_name))

        print("Components are : " + str(sequence_config["components"]))
        requests.put(local_url.format(api_host, sequence_name),
                     auth=(username, password),
                     params={"overwrite": "true"},
                     verify=not ('-i' in attributes),
                     headers={'Content-type': 'application/json'},
                     json={
                         'name': sequence_name,
                         'namespace': '_',
                         'exec': {
                             'kind': "sequence",
                             'components': sequence_config["components"]
                         },
                         'annotations': [
                             {"key": "web-export", "value": web},
                             {"key": "raw-http", "value": False},
                             {"key": "final", "value": True}
                         ],
                         "limits": {
                             "timeout": timeout,
                             "memory": memory,
                             "logs": logs,
                             "concurrency": concurrency
                         }
                     })


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
    for sequence_name in sequences.keys():
        response = requests.delete(local_url.format(api_host, sequence_name),
                                   auth=(username, password),
                                   verify=not ('-i' in attributes))
        print('DELETED - action: {} status: {}'.format(sequence_name, response.status_code))


def openwhisk_run(action_name):
    run_url = local_url
    attributes = ['-b', '-r']
    if 'ignore-certs' in provider.keys():
        if provider['ignore-certs'] is True:
            attributes.append('-i')

    try:
        # Check if Action is Web Action
        if 'web' in actions[action_name].keys():
            if actions[action_name]['web'] is True:
                run_url = web_url
    except:
        # Check if Sequence is Web Action
        if 'web' in sequences[action_name].keys():
            if sequences[action_name]['web'] is True:
                run_url = web_url
    response = requests.post(run_url.format(api_host, action_name),
                             auth=(username, password),
                             params={"blocking": "true", 'response': 'true'},
                             verify=not ('-i' in attributes))
    print(response.json()['response']['result'])


def openwhisk_logs(action_name):
    attributes = ['-b', '-r']
    if 'ignore-certs' in provider.keys():
        if provider['ignore-certs'] is True:
            attributes.append('-i')
    response = requests.get(activations_url.format(api_host),
                            auth=(username, password),
                            verify=not ('-i' in attributes),
                            json={"name": str(action_name)})
    activation_id = response.json()[1]["activationId"]
    response = requests.get(log_url.format(api_host, activation_id),
                            auth=(username, password),
                            verify=not ('-i' in attributes))
    print(response.json()['logs'])


def main():
    # OpenWhisk specific behaviour
    if deploy_config['provider']['platform'] == 'openwhisk':
        arguments = sys.argv[1:]
        if '--clear' in arguments:
            openwhisk_clear()
        elif '--run' in arguments:
            openwhisk_run(arguments[arguments.index('--run') + 1])
        elif '--logs' in arguments:
            openwhisk_logs(arguments[arguments.index('--logs') + 1])
        else:
            openwhisk_deployment()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("Could not connect to platform" + str(e))
