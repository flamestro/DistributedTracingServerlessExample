This is a example application to demonstrate distributed tracing in OpenWhisk. 
It uses a custom version of OpenWhisk which passes the __OW_TRACEID\_ as environment variable to every invoked container.

On un-customized OpenWhisk environments traces will only contain function side tracing data and no platform traces/spans.

This application uses a sampling rate of 100%

All the credentials are example credentials and all the endpoints are private/local endpoints.

## Requirements 
- python 3.7
- Running jaeger instance
- Running MinIO instance(s) with the buckets (`productstore` and `productimages`) and (`productdata`)
- Running OpenWhisk instance
## Setup
Install requirements with `pip3 install .`

Specify endpoints to your instances in the action .py files and in the deployless.yaml
All endpoints are initialized on under the action imports.

Then do
`python3 deployless.py`

To clear specified actions do
`python3 deployless.py --clear`

To run an action do
`python3 deployless.py --run <action-name>`

To get the latest logs of an action do
`python3 deployless.py --logs <action-name>`

## Requirements of Actions

This application uses [DeployLess](https://github.com/flamestro/deployless) to deploy actions.

DeployLess allows to specify action requirements inside the deployless.yaml file.
