const name = 'DataFlows';
const organization = 'mmiermans';
const domainPrefix = 'data-flows';
const isDev = process.env.NODE_ENV === 'development';
const environment = isDev ? 'Dev' : 'Prod';
const fullEnvironment = isDev ? 'development' : 'production';
const prefix = `${name}-${environment}`;
const domain = isDev
  ? `${domainPrefix}.miermans.help`
  : `${domainPrefix}.miermans.link`;
const graphqlVariant = isDev ? 'development' : 'current';
const githubConnectionArn = isDev
  ? 'arn:aws:codestar-connections:us-west-2:324770945281:connection/12027ec3-0c55-4146-a8e9-e12cca40ade0'
  : 'arn:aws:codestar-connections:us-west-2:324770945281:connection/12027ec3-0c55-4146-a8e9-e12cca40ade0';
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
    cpu: 4096, // 4096 = 4 vCPU
    memory: 30720, // 30720 = 30GB
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
    ],
    // Use the existing 'PocketDataProductReadOnly' policy. It currently only exists in production.
    // @see https://github.com/Pocket/data-shared/blob/main/lib/permissions-stack.ts#L14
    existingPolicies: isDev ? [] : ['PocketDataProductReadOnly'],
  },
};

export const config = {
  name,
  isDev,
  prefix,
  awsRegion: 'us-west-2',
  terraformBackend: {
    organization: organization,
  },
  s3BucketPrefix: organization,
  circleCIPrefix: `/${name}/CircleCI/${environment}`,
  shortName: 'DATAFL',
  environment,
  fullEnvironment,
  domain,
  prefect,
  codePipeline: {
    prefix: `${organization}-${prefix}`,
    githubConnectionArn,
    repository: 'miermans/data-flows',
    branch,
    codeDeploySnsTopicName: `Deployment-${environment}-ChatBot`,
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
