# data-flows
Data flows orchestrated using [Prefect](https://prefect.io).

## Architecture

- **ECS** runs Prefect Agent and Flow tasks
- **Docker image** stores Flow code and libraries
- **Auto-scaling** Prefect Agent
- **Permissions** according to the [principle of least privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege).

![Image](docs/images/Pocket_Prefect_Architecture.png?raw=true)

## Local development environment
We use two environments in this repo:
1. A Python environment, for writing Prefect Flows. Code is located in `src/`.
2. A Node environment for AWS infrastructure that Prefect needs to run Flows. Code is located in `.aws/`.

### 1. Local environment for developing Flows
Prerequisites:
- docker
- PyCharm

Steps:
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Decrypt and format your Snowflake private key. You'll use it in the next step when filling in the `.env` file.
   1. Run `openssl rsa -in ~/.snowflake/rsa_key.p8` and enter the passphrase for this file when prompted.
   2. Copy the value, after (but not including) `-----BEGIN RSA PRIVATE KEY-----` and before (not including) `-----END RSA PRIVATE KEY-----`.
   3. In a text editor, remove all newlines (`\n`).
3. Copy the `.env.example` file to a file in the same directory called `.env`. Change the values according to the instructions you find in that file. :warning: Do not put your credentials in `.env.example` to prevent accidentally checking them into git. Modifying `.env` is safe because it's git ignored.
4. Run `docker compose build && docker compose up`
5. In PyCharm, right-click on the _src_ directory > Mark Directory as > Sources Root
6. In PyCharm, [Configuring Docker Compose as a remote interpreter](https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#docker-compose-remote)

### 2. Local environment for AWS Infrastructure
Prerequisites (see [How to set up a Node.js development environment](https://getpocket.atlassian.net/wiki/spaces/PE/pages/2230321181/How+to+set+up+a+Node.js+development+environment)):
- node/npm
- nvm

Run the following commands in your terminal:
1. `cd .aws` to go to the .aws directory in the project root
2. `nvm use` to use the right Node version
3. `npm ci` to install packages from package-lock.json

## Initial Deployment
This section lists the manual steps that have to be taken
when this service is deployed to an AWS environment for the first time. 

### Prefect
Create a [Prefect project](https://docs.prefect.io/orchestration/concepts/projects.html)
with the name equal to the git branch name which will trigger the deployment.

### AWS SSM Parameter Store
The following parameters need to be created in the SSM Parameter Store.
Replace `{Env}` with the environment name as defined in
[.aws/src/config](https://github.com/Pocket/data-flows/blob/main/.aws/src/config/index.ts).

| Name                                             | Type         | Description                                                                                |
|--------------------------------------------------|--------------|--------------------------------------------------------------------------------------------|
| `/DataFlows/{Env}/PREFECT_API_KEY`       | SecureString | Prefect service account API key with 'user' permissions to the  previously created project |
| `/DataFlows/{Env}/SNOWFLAKE_PRIVATE_KEY` | SecureString | Decrypted base64 Snowflake private key                                                     |
| `/DataFlows/{Env}/SNOWFLAKE_ACCOUNT`     | String       | Snowflake account id                                                                       |
| `/DataFlows/{Env}/SNOWFLAKE_USER`        | String       | Snowflake username                                                                         |
| `/DataFlows/{Env}/DBT_CLOUD_TOKEN`       | SecureString | Dbt service account token                                                                  |
| `/DataFlows/{Env}/DBT_CLOUD_ACCOUNT_ID`  | String       | Dbt account id that you can find in the Dbt cloud url                                      |

## Productionizing a New Flow

Here are some things you'll want to do for using a flow in production:
- Get the flow into on-call alerts (instructions [here](https://github.com/Pocket/data-flows/wiki/Getting-a-new-Flow-into-On-Call-Alerts))

## Open questions
- Should we expire S3 results?
- Is it good practice to use flow results by reading from S3?

## References
- Experimental cloud account: https://cloud.prefect.io/mathijs-getpocket-com-s-account
- Running Prefect locally
  - [Prefect Getting Started](https://docs.prefect.io/orchestration/getting-started/quick-start.html)
- Running Prefect on AWS
  - [Prefect architecture diagram](https://docs.prefect.io/orchestration/#architecture-overview) 
  - [ECS Agent](https://docs.prefect.io/orchestration/agents/ecs.html#running-ecs-agent-in-production)
  - [ECS Agent CLI](https://docs.prefect.io/api/latest/cli/agent.html#ecs-start)
  - [ECSRun Run Configuration](https://docs.prefect.io/api/latest/run_configs.html#ecsrun)
  - [S3 Storage](https://docs.prefect.io/api/latest/storage.html#s3)
  - [Result Serializers](https://docs.prefect.io/api/latest/engine/serializers.html#serializer)
