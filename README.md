# data-flows
Data flows orchestrated using Prefect

## Local development environment
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Create a `.env` file in the project root with the following contents, using the above Prefect API key:
    ```
    PREFECT__CLOUD__API_KEY=********
    AWS_PROFILE=pocket-dev-PocketSSOBackend
    ```
