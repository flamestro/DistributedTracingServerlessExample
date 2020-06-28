## Config
create actions in the deploy-config.yaml as down below
```yaml
service_name: tracing-example

provider:
  platform: openwhisk
  ignore-certs: true
  api-host: 172.17.0.4:31001
  auth: 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP

actions:
  envtest:
    main: src/testfunctions/env-var-test/env-var-test.py
    kind: pythontracing:3
    dependencies: [src/utils/zipsendkin.py]
  hello-world:
    main: src/testfunctions/hello-world/hello-world.py
    kind: pythontracing:3
  requirements-test:
    requirements: src/testfunctions/requirements-test/requirements.txt
    main: src/testfunctions/requirements-test/requirements-test.py
    kind: pythontracing:3
  zipkin-test:
    main: src/testfunctions/zipkin-test/zipkin-test.py
    kind: pythontracing:3
    dependencies: [src/utils/zipsendkin.py]
```
## Setup
`python deploy.py`

`python deploy.py --clear`

`python deploy.py --run <action-name>`

## Requirements

Actions can have custom requirements which need to be specified in the deployless.yaml.

The Project can have development requirements under the root of the project.
