// NODE_ENV envar will control what TF Workspace we are using
const isDev = process.env['NODE_ENV'] === 'development';
const region = process.env['AWS_REGION'];
const workspaceName = isDev ? 'prefect-v2-dev' : 'prefect-v2-production';
const environment = isDev ? 'dev' : 'production';
const service = 'Prefect v2';
const team = 'Data Products';
const log_retention_days = 30;
const agentCpu = 1024;
const agentMemory = 2048;
const agentTaskCount = 1;
const agentImage = 'prefecthq/prefect:2-python3.10';
const OIDCOrgId = process.env['OIDC_ORG_ID'] || '';
const OIDCProjectId = process.env['OIDC_PROJECT_ID'] || '';

export const config = {
  workspaceName,
  isDev,
  region,
  tags: {
    environment: environment,
    service: service,
    team: team
  },
  log_retention_days,
  agentCpu,
  agentMemory,
  agentTaskCount,
  agentImage,
  OIDCOrgId,
  OIDCProjectId
};
