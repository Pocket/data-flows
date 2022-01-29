# data-flows
Data flows orchestrated using [Prefect](https://prefect.io).

## Architecture

- **ECS** runs Prefect Agent and Flow tasks
- **Docker image** stores Flow code and libraries
- **Auto-scaling** Prefect Agent based on CPU usage
- **Private subnet** shields tasks from the internet; only egress is needed 
- **Permissions** according to the [principle of least privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege).

![Image](docs/images/Pocket_Prefect_Architecture.png?raw=true)

## Local development
We use two environments in this repo:
1. A Python environment, for writing Prefect Flows. Code is located in `src/`.
2. A Node environment for AWS infrastructure that Prefect needs to run Flows. Code is located in `.aws/`.

### 1. Developing Flows

####Prerequisites:
- docker
- PyCharm

#### One-time setup of your local environment:
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Decrypt and format your Snowflake private key. You'll use it in the next step when filling in the `.env` file.
   1. Run `openssl rsa -in ~/.snowflake/rsa_key.p8` and enter the passphrase for this file when prompted.
   2. Copy the value, after (but not including) `-----BEGIN RSA PRIVATE KEY-----` and before (not including) `-----END RSA PRIVATE KEY-----`.
   3. In a text editor, remove all newlines (`\n`).
3. Copy the `.env.example` file to a file in the same directory called `.env`. Change the values according to the instructions you find in that file. :warning: Do not put your credentials in `.env.example` to prevent accidentally checking them into git. Modifying `.env` is safe because it's git ignored.
4. Run `docker compose build && docker compose up`
5. In PyCharm, right-click on the _src_ directory > Mark Directory as > Sources Root
6. In PyCharm, [Configuring Docker Compose as a remote interpreter](https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#docker-compose-remote)

#### Running Flows:
1. :warning: Even when flows are executed locally they can affect production resources.
Check whether the `AWS_PROFILE` variable in your `.env` file has write access in production, and if so,
consider whether the flow you're going to run could have unintended consequences. Ask if you're not sure.
2. Run `source <(maws -o awscli)` and choose the AWS account that matches the value of `AWS_PROFILE` in your `.env` file.
3. Run the flow in PyCharm, for example by right-clicking on the corresponding file in the `src/flows/` directory and choosing 'Run'.

#### Installing additional libraries
Libraries are managed using [pipenv](https://pipenv.pypa.io/en/latest/), to create a consistent run-time environment.
Follow the steps below to install a new Python library. 

1. Run `docker compose up`.
2. In a new terminal, run `docker compose exec prefect pipenv install pydantic`, replacing `pydantic` with the library you want to install.
   1. Add `--dev` for development libraries that don't need to be installed on production, for example `docker compose exec prefect pipenv install --dev pytest`.
3. The output should look something like this:
```shell
$ docker compose exec prefect pipenv install pydantic
Installing pydantic...
Adding pydantic to Pipfile's [packages]...
✔ Installation Succeeded 
Installing dependencies from Pipfile.lock (46b380)...
  🐍   ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 68/68 — 00:00:20
```
3. Run `docker compose cp prefect:/Pipfile ./ && docker compose cp prefect:/Pipfile.lock ./` to copy the Pipenv files from your Docker container to your host.
   1. Note: if you get `No such command: cp`, try upgrading Docker, or use [docker cp](https://docs.docker.com/engine/reference/commandline/cp/) instead.
4. `Pipfile` and `Pipfile.lock` should have been changed. Commit those changes to git.
5. Run `docker compose build` to rebuild your Docker image. 

### 2. Developing AWS Infrastructure

#### Prerequisites:
- node/npm and nvm (see [How to set up a Node.js development environment](https://getpocket.atlassian.net/wiki/spaces/PE/pages/2230321181/How+to+set+up+a+Node.js+development+environment))
- tfenv

#### Install Node packages:
1. `cd .aws` to go to the .aws directory in the project root
2. `nvm use` to use the right Node version
3. `npm ci` to install packages from package-lock.json

#### Manually applying infrastructure changes in Pocket-Dev
Pushing to the `dev` branch (see 'Deployment' below) is an easy way to deploy infrastructure changes to Pocket-Dev.
The steps below are useful if you want to iterate more quickly over changes in the `.aws/` directory.

1. Run `$(maws)` and obtain write access to Pocket-Dev
2. Run `tfenv use` to get the right version of Terraform
3. Run `npm run build:dev`
4. From the `.aws/` directory, run `cd cdktf.out/stacks/data-flows/`
5. Run `terraform init` and choose 'Dev'
6. Run `npm run build:dev && terraform apply`. Repeat this step when you want to apply changes.

## Productionizing a New Flow

Here are some things you'll want to do for using a flow in production:
- Get the flow into on-call alerts (instructions [here](https://github.com/Pocket/data-flows/wiki/Getting-a-new-Flow-into-On-Call-Alerts))
- Log important metrics (for example number of rows)
- Throw exceptions for invalid input
- Usually flows will run on a [schedule](https://docs.prefect.io/core/concepts/schedules.html#overview)

## Deployment

- Pocket-Dev: `git push -f origin my-local-branch:dev`
- Production: get your PR approved, and merge it into the main branch

Deployments take about 15 minutes. You can monitor their progress in
[CircleCI](https://app.circleci.com/pipelines/github/Pocket/data-flows)
and
[CodePipeline](https://console.aws.amazon.com/codesuite/codepipeline/pipelines?region=us-east-1&pipelines-meta=eyJmIjp7InRleHQiOiJEYXRhRmxvd3MifSwicyI6eyJwcm9wZXJ0eSI6InVwZGF0ZWQiLCJkaXJlY3Rpb24iOi0xfSwibiI6MTAsImkiOjB9).

### Initial Deployment
This section lists the manual steps that have to be taken
when this service is deployed to an AWS environment for the first time. 

### 1. Prefect
Create a [Prefect project](https://docs.prefect.io/orchestration/concepts/projects.html)
with the name equal to the git branch name which will trigger the deployment.

### 2. AWS SSM Parameter Store
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

## Roadmap

- Data validation
- Persist [Prefect results](https://docs.prefect.io/core/concepts/results.html) to S3
- Automated integration tests
- Python linter
- Sentry integration
- Switch to the [LocalDaskExecutor](https://docs.prefect.io/api/latest/executors.html#localdaskexecutor) to allow tasks to be executed in parallel

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
