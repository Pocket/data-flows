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

To install as part of your data-flows project, you can do one of 
2 things:

- Reference the path in your pyproject.toml and run `poetry install`

```toml
[tool.poetry]
name = "prefect-flows"
version = "0.1.0"
description = ""
authors = ["Braun <breyes@mozilla.com>"]
readme = "README.md"

[tool.poetry.dependencies]
python = "^3.10"
common-utils = {path = "../common-utils"}


[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

- Build a .whl file and install it directly.  The Poetry build command will create a `dist` folder with the .whl file in it.

```bash
# From the common-utils directory
poetry build
```

```
dist
├── common_utils-0.1.0-py3-none-any.whl
└── common_utils-0.1.0.tar.gz
```

> :heavy_exclamation_mark: This package will not install with any external dependencies.  It is meant to be used with the versions of Prefect and Prefect-AWS that you are using in your project.  However, you can install the `dev` dependency group to install the libraries used for development.

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

Per the [Prefect v2 docs](https://docs.prefect.io/), you will need to have your `PREFECT_API_KEY` and `PREFECT_API_URL` environment variables set.

You will also need to have your AWS credentials set either via Environment Variables, credientials file,  or SSO.  With SSO, you will most likely need your `AWS_DEFAULT_REGION` environment variable set.

The main deliverable here is the `deploy-cli`.  This will be installed in your Poetry Python environment.

Here is the help text:

```
deploy-cli -h
usage: deploy-cli [-h] [--build-only]

Running this cli directly will:
    - Build and push your Docker images to our private ECR repository.
    - Create an S3 filesytem block in Prefect v2 for storing flows and
    for storing flow run staging/working files.
    

options:
  -h, --help    show this help message and exit
  --build-only  Set this flag to only run Docker build.
```

The purpose of this CLI is to:

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

COPY pyproject.toml /src/repo/
COPY poetry.lock /src/repo/

WORKDIR /src/repo

RUN $HOME/.local/bin/poetry config virtualenvs.create false
RUN $HOME/.local/bin/poetry install --no-root --only dev
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
f"{project_name}-prod-test"
```
```python
f"{project_name}-prod-live"
```

`test` will be used for flows pushed to the `dev-v2` branch.

`prod` will be used for flows pushed to the `main-v2` branch.

## Contributing

PRs from the Pocket Team are welcomed given that changes to made with the understanding that this is meant to be **shared** for code Prefect v2 flows.

## Notes

More context will be available on how this project works in the wild once we have the flow deployment tools added with the [prefect-flows](../prefect-flows/) subproject built out as an example.

## Links

More links to come...


## Licensing

The code in this project is licensed under Apache-2.0 license.
