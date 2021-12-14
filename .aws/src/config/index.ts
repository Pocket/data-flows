const name = 'DataFlows';
const domainPrefix = 'data-flows';
const isDev = process.env.NODE_ENV === 'development';
const environment = isDev ? 'Dev' : 'Prod';
const domain = isDev
  ? `${domainPrefix}.getpocket.dev`
  : `${domainPrefix}.readitlater.com`;
const graphqlVariant = isDev ? 'development' : 'current';
const githubConnectionArn = isDev
  ? 'arn:aws:codestar-connections:us-east-1:410318598490:connection/7426c139-1aa0-49e2-aabc-5aef11092032'
  : 'arn:aws:codestar-connections:us-east-1:996905175585:connection/5fa5aa2b-a2d2-43e3-ab5a-72ececfc1870';
const branch = isDev ? 'dev' : 'main';
const prefect = {
  api: 'https://api.prefect.io',  // Use Prefect Cloud
  port: 8080,  // Port for health check server
  agentLabels: [environment],
  agentLevel: isDev ? 'DEBUG' : 'INFO',
  runTaskRole: {
    dataLearningBucketName: isDev ? 'pocket-data-learning-dev' : 'pocket-data-learning',
    // Use the existing 'PocketDataProductReadOnly' policy. It currently only exists in production.
    // @see https://github.com/Pocket/data-shared/blob/main/lib/permissions-stack.ts#L14
    existingPolicies: isDev ? [] : ['PocketDataProductReadOnly']
  }
}

export const config = {
  name,
  isDev,
  prefix: `${name}-${environment}`,
  circleCIPrefix: `/${name}/CircleCI/${environment}`,
  shortName: 'DATAFL',
  environment,
  domain,
  prefect,
  codePipeline: {
    githubConnectionArn,
    repository: 'pocket/data-flows',
    branch,
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
