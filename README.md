# data-flows
Data flows orchestrated using Prefect

## Local development environment
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Create a `.env` file in the project root with the following contents, using the above Prefect API key:
    ```
    PREFECT__CLOUD__API_KEY=********
    AWS_PROFILE=pocket-dev-PocketSSOBackend
    AWS_DEFAULT_REGION=us-east-1
    PREFECT_TASK_ROLE_ARN=the task role (e.g. DataFlows-Dev-RunTaskRole) you want your tasks to use
    ```
3. (Optional) Install Python 3.9 using [pyenv](https://github.com/pyenv/pyenv). 
4. Run `pipenv install` in the project root directory.

## Cloud environment
URL: https://cloud.prefect.io/mathijs-getpocket-com-s-account
