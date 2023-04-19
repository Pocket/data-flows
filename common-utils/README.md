# Common Utilities for Prefect v2
> Standardization of CICD and common/repeatable flow components.

The goal of this project is to serve as a shared code repository for common repeatable workflows for Prefect v2 flows at Pocket.

These things include:
- CI/CD patterns
- Connecting to Cloud Services and Databases
- Enforcing other conventions specific to Pocket Prefect flows

This project is a part of the larger **data-flows** project and meant to be installed as a dependency in other prefect flow sub-projects.

## Getting Started

Once you have cloned the larger **data-flows** project.  You should be working in the `common-utils` folder.

This project uses Poetry for dependency management.  You will want to follow the installation instructions here: 

https://python-poetry.org/docs/#installation

This project also uses dependency groups for managing Python dependencies.  You can learn about them here:

https://python-poetry.org/docs/managing-dependencies/#dependency-groups

### Package User

To install as part of your data-flows project, here are the steps:

- From the root of your sub-project run `../common-utils/common_utils_build.sh`.  This will create a `dist` folder with the proper wheel file you need to install.

- Finally run `poetry add dist/<wheel file name>.whl`

The dist folder is in the .gitignore because the CI/CD job will run the `../common-utils/common_utils_build.sh` for you.  This means that your poetry install will work as expected.  It also means the build will fail if you have not installed the latest version of the library.

> :heavy_exclamation_mark: This library currently requires the use of our custom Prefect-AWS package at https://github.com/Pocket/prefect-aws.git@test-prs.  We needed the changes outlined in these PRs: https://github.com/PrefectHQ/prefect-aws/pull/236 and 
https://github.com/PrefectHQ/prefect-aws/pull/233 (released).
Once both of these changes are released through the main project, we can revert back to the main library.

### Package Developer

To setup your dev environment for developing on the project, simply run Poetry install for the dev dependency group.  

```bash
# Installs dev dependencies and any cli scripts
poetry install --only main, dev
```

Tests can be executed via Pytest.

```bash
python -m pytest --cov=src/common --cov-report term-missing --cov-fail-under=100
```

Changes to the code will trigger the **build** process:

- Black syntax check
- Check for unit test completion and coverage

CI/CD workflow is managed by CircleCI.  The workflow code is [here](../.circleci).

## Features

Below is a high level overview of the current modules available.

### Development Module

This module provides tooling for deploying your project and flow resources to AWS and Prefect respectively.

Per the [Prefect v2 docs](https://docs.prefect.io/), you will need to have your `PREFECT_API_KEY` and `PREFECT_API_URL` environment variables set when using the module for pushing flows to Prefect Cloud.

You will also need to have your AWS credentials set either via Environment Variables, credientials file,  or SSO.  With SSO, you will most likely need your `AWS_DEFAULT_REGION` environment variable set.

Another important environment variable is `DEPLOYMENT_TYPE`.  This determines what infrastructure flows run on as well as what access it will have.

`DEPLOYMENT_TYPE` has to do with Prefect Flow development and underlying AWS infrastructure.  Flows merged to `main-v2` will be considered a `main` deployment type and flows optionally merged to `staging-v2` will be considered a `staging` deployment type.  There will be deployment and infrastructure resources available to support both in the Prefect Environment.  There will be the option to push flows to a `dev` deployment type powered by the AWS development account.  This is no automation for this, so though flow code will be pulled from `dev-v2`, a push to the `dev-v2` branch will not trigger a flow deployment.  However, there will be an option to deploy flows locally as needed to the `dev` deployment type.

The main deliverable here is the `deploy-cli`.  This will be installed in your Poetry Python environment.  This provides the commands needed to leverage the deployment tools capabilities.

Finally, this module relies on the existence of a Poetry PyProject path to pull metadata for deployment.  Here is a code sample:

```python
PYPROJECT_FILE_PATH = os.path.expanduser(os.path.join(os.getcwd(), "pyproject.toml"))
```

We default to using current working directory to find the pyproject.toml file.  For the `deploy-cli` to work, you must execute it from the subproject root.

Here is the help text:

```
usage: deploy-cli [-h] [--check-version]  ...

basic CLI setup for running deployment utils as needed.
    

options:
  -h, --help       show this help message and exit
  --check-version  Do a version check against main-v2 for current project.

subcommands:
                   these are the subcommands to use for deploying flows and environments as needed
    deploy-envs    use this command to deploy the docker envs configured in your pyproject.toml
                   this will also deploy your project's filesystem.
                           
    deploy-flows   use this command to deploy flows from the flows_folder configured in your pyproject.toml
```

As you can see, there are 2 subcommands:

deploy-envs

```
usage: deploy-cli deploy-envs [-h] [--build-only]

options:
  -h, --help    show this help message and exit
  --build-only  set this flag to only run Docker build.
```

deploy-flows

```
usage: deploy-cli deploy-flows [-h] [--validate-only]

options:
  -h, --help       show this help message and exit
  --validate-only  set this flag to only validate flow specs.
```

There is also a top level flag called `--check-version`

This will pull down a project `pyproject.toml` file and make sure the version is update as needed if there are changes.

We leverage this in our CICD process.

#### **Docker Environments**

The purpose of `deploy-envs` is to accept and create a series of environment configurations that will used as Docker environments for your flow runs.

This is the environment that your flow will run in.  It can be defined however you want given it has the proper top level template for deriving from a support Prefect v2 Docker Images.

```Dockerfile
ARG PYTHON_VERSION
FROM prefecthq/prefect:2-python${PYTHON_VERSION}

RUN apt update && \
    apt install -y curl

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY dist /src/repo/dist 
COPY pyproject.toml /src/repo/
COPY poetry.lock /src/repo/

WORKDIR /src/repo

RUN $HOME/.local/bin/poetry config virtualenvs.create false
RUN $HOME/.local/bin/poetry install --no-root --only main
```

As you can see, we want to make sure we are using supported Prefect Docker Images.  This is useful even though your own project may install a different version of Prefect.

These environments will be defined in your Poetry `pyproject.toml`.

```toml
[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"

[tool.prefect.envs.base]
python_version = "3.10"
dockerfile_path=".docker/main/Dockerfile"
docker_build_context="<code directory>"  # this is optional and will default to '.'
```

These are project level images that can be leveraged by any flow.

#### **Flow Deployment and Deployment Configurations**

The purpose of this `deploy-flows` is to:

- Using the `tool.prefect.flows_folder` configuration, find Python files that end in `_flow.py` and attempt to deploy them to Prefect.
- The deployment of a flow consists of:
    - Registering the proper ECS Task Definition.
    - Creating an ECS Task Block in Prefect that defines the flow's task definition.
    - Registering the flow's deployment configurations with Prefect.  A flow can have multiple deployment configurations.  Deployment documentation is found [here](https://docs.prefect.io/concepts/deployments/).

We will be leveraging Github for flow storage.  This works well because we can map our deployments and agent queues to Github branches.

Branch `main-v2` will be used for releasing production ready flow deployments.  These will run on AWS production infrastructure using the `main` Prefect agent.

Branch `staging-v2` will be used for pre-release flow deployments.  These will run on AWS production infrastructure using the `staging` Prefect agent.  The idea here is that these flows should only have access to leverage resources approved for staging.  This will be done via a combination of IAM access and loading certain credentials via secrets manager.

The `dev-v2` can be used to test deployments locally against a local Prefect instance or Prefect Cloud via the `dev` agent.  Flow deployments submitted via this agent will run in the AWS development account.


The folder to look for flows is defined in the pyproject.toml.

```toml
[tool.prefect]
flows_folder = "tests/test_flows"
```

Once a flow file is found, we look for a global variable in the file called `FLOW_SPEC`.

This variable must be an instantiated `FlowSpec` object.

```python
from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

from common.deployment import FlowDeployment, FlowEnvar, FlowSpec


@task()
def task_1():
    print("hello world")


@flow()
def flow_1():
    task_1()


FLOW_SPEC = FlowSpec(
    flow=flow_1,
    docker_env="base",
    secrets=[
        FlowEnvar(envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret")
    ],
    ephemeral_storage_gb=200,
    deployments=[
        FlowDeployment(
            deployment_name="base",
            cpu="1024",
            memory="4096",
            parameters={"param_name": "param_value"},
            schedule=CronSchedule(cron="0 0 * * *"),
            envars=[
            FlowEnvar(envar_name="EXTRA_PIP_PACKAGES", envar_value="pandas")
    ],
        )
    ],  # type: ignore
)
```

The models are Pydantic models, so the inputs are validated.

`FlowSpec` fields as a whole, control how the task definition is created.  Any AWS ARN/URI specifics are added for you.  The `docker_env` must exist in the pyproject.toml.

`FlowDeployment` controls how the Prefect Deployments are registered.

Screenshots can be found [here](https://getpocket.atlassian.net/wiki/spaces/PE/pages/2917105700/Prefect+v2+CI+CD#Flow-Deployment-Example).

## Contributing

PRs from the Pocket Team are welcomed given that changes are made with the understanding that this is meant to be **shared** code Prefect v2 flows.

## Notes

A subproject example can be found in the [data-products](../data-products) project folder at the root of the git repo.  This is where the Pocket Data Products Team will author flows.


## Links

More links to come...


## Licensing

The code in this project is licensed under Apache-2.0 license.
