const workspaceName =
  process.env['TF_WORKSPACE'] !== undefined
    ? process.env['TF_WORKSPACE']
    : 'prefect-v2-dev';
const isDev = workspaceName === 'prefect-v2-dev';
const environment = isDev ? 'dev' : 'production';
const service = 'Prefect v2';
const team = 'Data Products';
const version =
  process.env['PACKAGE_VERSION'] !== undefined
    ? process.env['PACKAGE_VERSION']
    : 'dev';
const region = 'us-east-1';
const log_retention_days = 30;
const agentCpu = '1024';
const agentMemory = '2048';
const agentTaskCount = 1;
const agentImage = 'prefecthq/prefect:2-python3.10';
const circleCIDevWorkspaceName = 'prefect-v2-circleci-dev';
const isLocal = process.env['DPT_IS_LOCAL'] === 'true';
const testCircleCIArn = process.env['DPT_LOCAL_CIRCLECI_ARN'];

export const config = {
  workspaceName,
  isDev,
  region,
  tags: {
    environment: environment,
    service: service,
    team: team,
    version: version
  },
  log_retention_days,
  agentCpu,
  agentMemory,
  agentTaskCount,
  agentImage,
  circleCIDevWorkspaceName,
  isLocal,
  testCircleCIArn
};
