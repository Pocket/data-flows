# data-flows
Data flows orchestrated using Prefect

## Local development
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Create a `.env` file in the project root with the following contents, using the above Prefect API key:
    ```
    PREFECT__CLOUD__API_KEY=********
    AWS_PROFILE=pocket-dev-PocketSSOBackend
    AWS_DEFAULT_REGION=us-east-1
    PREFECT_TASK_ROLE_ARN=the task role (e.g. DataFlows-Dev-RunTaskRole) you want your tasks to use
    ```
3. Choose how to run code:
   1. Docker compose: consistent environment
   2. pipenv: fast startup

### Option 1: Docker compose
Prerequisites:
- docker

Steps:
1. Run `docker compose build && docker compose up`
2. In PyCharm, [Configuring Docker Compose as a remote interpreter](https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#docker-compose-remote)

### Option 2: PyCharm and pipenv
Prerequisites:
- pipenv
- python 3.9 ([pyenv](https://github.com/pyenv/pyenv) makes it easy to manage Python versions)

Steps:
1. Run `pipenv install` in the project root directory.
2. In PyCharm, [configure pipenv as the interpreter](https://www.jetbrains.com/help/pycharm/pipenv.html#pipenv-existing-project).

## Cloud environment
URL: https://cloud.prefect.io/mathijs-getpocket-com-s-account
