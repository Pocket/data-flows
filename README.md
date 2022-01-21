# data-flows
Data flows orchestrated using Prefect

## Local development
1. Create a Prefect API key on the [API keys page](https://cloud.prefect.io/user/keys).
2. Copy the `.env.example` file to a file in the same directory called `.env`. Change the values according to the instructions you find in that file. :warning: Do not put your credentials in `.env.example` to prevent accidentally checking them into git. Modifying `.env` is safe because it's git ignored.
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

## Initial Deployment
The following manual steps are required when this service is deployed
to an AWS environment for the first time (replace `{environment}` with the environment name):
- Create SSM Parameter `/DataFlows/{environment}/PREFECT_API_KEY` with the Prefect API key.

## Road map

### CI/CD
As we're experimenting with Prefect we've deployed flows from our local machines. When we productionalize Prefect,
we'll want to automate this. It might look something like this: 

1. Set up Prefect projects for each environment (Prod, Dev).
2. Set values in Parameter Store that tell CodeBuild which Prefect project to use, keyed on branch name.
In [dl-metaflow-jobs's buildspec.yml](https://github.com/Pocket/dl-metaflow-jobs/blob/main/buildspec.yml)
we have a similar pattern, but we assume there's only one deployment per AWS account.
3. Collect all flows, and for each flow:
   1. Set the storage and run configuration.
   2. Register the flow with Prefect.

There's [a Github discussion on Prefect CI/CD patterns](https://github.com/PrefectHQ/prefect/discussions/4042)
with more details and more patterns.

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
