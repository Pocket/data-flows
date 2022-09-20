const name = 'DataFlows';
const domainPrefix = 'data-flows';
const s3BucketPrefix = 'pocket';
const isDev = process.env.NODE_ENV === 'development';
const environment = isDev ? 'Dev' : 'Prod';
const fullEnvironment = isDev ? 'development' : 'production';
const domain = isDev
  ? `${domainPrefix}.getpocket.dev`
  : `${domainPrefix}.readitlater.com`;
const graphqlVariant = isDev ? 'development' : 'current';
const githubConnectionArn = isDev
  ? 'arn:aws:codestar-connections:us-east-1:410318598490:connection/7426c139-1aa0-49e2-aabc-5aef11092032'
  : 'arn:aws:codestar-connections:us-east-1:996905175585:connection/5fa5aa2b-a2d2-43e3-ab5a-72ececfc1870';
const branch = isDev ? 'dev' : 'main';

// Git branch name is used to determine which Prefect project to register the tasks in: dev or prod.
// In the future we might support feature deployments where the project is created automatically during deployment.
const prefectProjectName = branch;
const prefect = {
  api: 'https://api.prefect.io', // Use Prefect Cloud
  port: 8080, // Port for health check server
  projectName: prefectProjectName,
  agentContainerName: 'app',
  agentLabels: [prefectProjectName],
  agentLevel: isDev ? 'DEBUG' : 'INFO',
  // The flowTask object below configures the ECS tasks that execute the Prefect flows.
  flowTask: {
    // See the documentation below for valid values for CPU and memory:
    // https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-ecs-taskdefinition.html#cfn-ecs-taskdefinition-cpu
    cpu: 4096,
    memory: 32768,
    dataLearningBucketName: isDev
      ? 'pocket-data-learning-dev'
      : 'pocket-data-learning',
    // To securely inject an environment variable FOO_BAR in the ECS task that executes Prefect Flows, add 'FOO_BAR' to
    // the list below and create Parameters /DataFlows/Prod/FOO_BAR and /DataFlows/Dev/FOO_BAR in Prod and Dev.
    parameterStoreNames: [
      'SNOWFLAKE_PRIVATE_KEY',
      'SNOWFLAKE_ACCOUNT',
      'SNOWFLAKE_USER',
      'DBT_CLOUD_TOKEN',
      'DBT_CLOUD_ACCOUNT_ID',
      'GCE_KEY',
      'BRAZE_API_KEY',
      'BRAZE_REST_ENDPOINT',
      'SNOWFLAKE_DATA_RETENTION_ROLE',
      'SNOWFLAKE_DATA_RETENTION_WAREHOUSE',
      'SNOWFLAKE_DATA_RETENTION_DB',
      'SNOWFLAKE_DATA_RETENTION_SCHEMA',
      'SNOWFLAKE_SNOWPLOW_DB',
      'SNOWFLAKE_SNOWPLOW_SCHEMA',
      'SNOWFLAKE_RAWDATA_DB',
      'SNOWFLAKE_RAWDATA_FIREHOSE_SCHEMA',
      'SNOWFLAKE_SNAPSHOT_DB',
      'SNOWFLAKE_SNAPSHOT_FIREHOSE_SCHEMA',
      'POCKET_PUBLISHER_DATABASE_HOST',
      'POCKET_PUBLISHER_DATABASE_PORT',
      'POCKET_PUBLISHER_DATABASE_DBNAME',
      'POCKET_PUBLISHER_DATABASE_USER',
      'POCKET_PUBLISHER_DATABASE_PASSWORD',
    ],
    // Use the existing 'PocketDataProductReadOnly' policy. It currently only exists in production.
    // @see https://github.com/Pocket/data-shared/blob/main/lib/permissions-stack.ts#L14
    existingPolicies: isDev ? [] : ['PocketDataProductWriteAccess'],
  },
};

export const config = {
  name,
  isDev,
  prefix: `${name}-${environment}`,
  awsRegion: 'us-east-1',
  s3BucketPrefix,
  circleCIPrefix: `/${name}/CircleCI/${environment}`,
  shortName: 'DATAFL',
  environment,
  fullEnvironment,
  domain,
  prefect,
  terraform: {
    organization: 'Pocket',
  },
  codePipeline: {
    artifactBucketPrefix: `${s3BucketPrefix}-codepipeline`,
    githubConnectionArn,
    repository: 'pocket/data-flows',
    branch,
    codeDeploySnsTopicName: `DataAndLearning-${environment}-ChatBot`,
  },
  graphqlVariant,
  healthCheck: {
    command: [
      'CMD-SHELL',
      // Prefect exposes a simple health check: https://docs.prefect.io/orchestration/agents/overview.html#health-checks
      // The Prefect Docker image (based on python:3.9-slim) does not have curl or wget installed, so we'll use Python
      // to make a request to the health check endpoint.
      `python -c 'import requests; requests.get("http://localhost:${prefect.port}/api/health").raise_for_status()' || exit 1`,
    ],
    interval: 15,
    retries: 3,
    timeout: 5,
    startPeriod: 0,
  },
  tags: {
    service: name,
    environment,
  },
};
