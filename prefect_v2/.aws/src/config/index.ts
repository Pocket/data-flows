// NODE_ENV envar will control what TF Workspace we are using
const isDev = process.env.NODE_ENV === 'development';
const workspaceName = isDev ? 'prefect-v2-dev' : 'prefect-v2-production';
const environment = isDev ? 'dev' : 'production';
const service = 'Prefect v2';
const team = 'Data Products';
// the PACKAGE_VERSION envar will be used to tag resources properly
const version =
  process.env['PACKAGE_VERSION'] !== undefined
    ? process.env['PACKAGE_VERSION']
    : 'dev';
const log_retention_days = 30;
const agentCpu = '1024';
const agentMemory = '2048';
const agentTaskCount = 1;
const agentImage = 'prefecthq/prefect:2-python3.10';
// these final configs allow the testing of the CircleCI OpenID Role in Dev
const circleCIDevWorkspaceName = 'prefect-v2-circleci-dev';
const isLocal = process.env['DPT_IS_LOCAL'] === 'true';
const testCircleCIArn = process.env['DPT_LOCAL_CIRCLECI_ARN'];

export const config = {
  workspaceName,
  isDev,
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
