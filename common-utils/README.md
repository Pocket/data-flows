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

There 2 more important environment variables: `ENVIRONMENT_TYPE` (dev or production) and `DEPLOYMENT_TYPE` (test or live).

`ENVIRONMENT_TYPE` has to do with the environment that we are deploying underlying resources to.  For example, Infrastructure and common-utils will be developed and tested in the `dev` environment and then deployed using the `production` environment.  These resources make up the "Platform".

`DEPLOYMENT_TYPE` has to do with Prefect Flow development.  Flows merged to `main-v2` will be considered `live` and flows optionally merged to `test-v2` will be considered `test`.  There will be deployment and infrastructure resources available to support both in the Prefect Environment.

This will be explained in more detail on this repository's top level [README.md](../README.md)

Finally, this module relies on the existence of a Poetry PyProject path to pull metadata for deployment.  Here is a code sample:

```python
# config for supporting using different pyproject.toml path when developing flows
CWD_DIR = os.path.join(os.getcwd(), "pyproject.toml")

PYPROJECT_PATH = os.path.abspath(
    os.path.expanduser(os.getenv("PREFECT_PYPROJECT_PATH", CWD_DIR))
)
```

We default to using current working directory, which will most likely be the root of the flows project, to find the pyproject.toml file.

> We recommend that your working directory for flow development be the root of your project.  However, below there is an escape hatch envar you can use for local flow development.

There maybe situation during development where your working directory is not where the pyproject.toml is, you can set the path directly via the `PREFECT_PYPROJECT_PATH` environment variable.

Since the deployment module imports data from the pyproject.toml for usage, the module will need access to this file.  The envar helps with that.

We use this escape hatch for running the prefect cli since those commands need to run from the directory of the flow files.

> NOTE: Setting this envar when running the deployment tools will result in errors because all other path config values in the pyproject.toml are expected to be absolute or relative to the current working directory.

The main deliverable here is the `deploy-cli`.  This will be installed in your Poetry Python environment.  This provides the commands needed to leverage the deployment tools capabilities.

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

The purpose of `deploy-envs` is to:

- Accept and create a series of environment configurations that will used as Docker environments for your flow runs.
- Create a [Prefect Filesystem](https://docs.prefect.io/concepts/filesystems/#s3) for your Project.  This filesystem is used as [Remote Storage](https://docs.prefect.io/concepts/storage/) for your flow code.  You will also be able to use this filesystem in your flow logic for storing files as needed.

#### **Docker Environments**

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

#### **Filesystems**

Each project will get 2 filesystems:

```python
f"{project_name}-production-test"
```
```python
f"{project_name}-production-live"
```

`test` will be used for flows pushed to the `test-v2` branch.

`production` will be used for flows pushed to the `main-v2` branch.

The `dev-v2` will be reserved for testing platform components like `.aws` and CICD.

The purpose of this `deploy-flows` is to:

- Using the `tool.prefect.flows_folder` configuration, find Python files that end in `_flow.py` and attempt to deploy them to Prefect.
- The deployment of a flow consists of:
    - Registering the proper ECS Task Definition.
    - Creating an ECS Task Block in Prefect that defines the flow's task definition.
    - Loading the flow files to the S3 filessystem.
    - Registering the flow's deployment configurations with Prefect.  A flow can have multiple deployment configurations.  Deployment documentation is found [here](https://docs.prefect.io/concepts/deployments/).

#### **Flow Deployment and Deployment Configurations**

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

from common.deployment import FlowDeployment, FlowSecret, FlowSpec


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
        FlowSecret(envar_name="MY_SECRET_JSON", secret_name="/my/secretsmanager/secret")
    ],
    ephemeral_storage_gb=200,
    deployments=[
        FlowDeployment(
            deployment_name="base",
            cpu="1024",
            memory="4096",
            parameters={"param_name": "param_value"},
            schedule=CronSchedule(cron="0 0 * * *"),
        )
    ],  # type: ignore
)
```

They models are Pydantic models, so the inputs are validated.

`FlowSpec` fields `[flow, docker_env, secrets, ephemeral_storage_gb]` control how the task definition is created.  Any AWS ARN/URI specifics are added for you.  The `docker_env` must exist in the pyproject.toml.

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
