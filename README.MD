## Requirements 
- python 3.7

## Setup
Install requirements with `pip3 install .`

Then do
`python deployless.py`

To clear specified actions do
`python deployless.py --clear`

To run an action do
`python deployless.py --run <action-name>`

To get the latest logs of an action do
`python deployless.py --logs <action-name>`

## Requirements

Actions can have custom requirements which need to be specified in the deployless.yaml.

The Project can have development requirements under the root of the project.
