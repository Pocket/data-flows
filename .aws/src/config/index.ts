// NODE_ENV envar will control what TF Workspace we are using
const isDev = process.env['NODE_ENV'] === 'development';
const region = process.env['AWS_REGION'];
const environment = isDev ? 'dev' : 'production';
const service = 'Prefect v2';
const team = 'Data Products';
const log_retention_days = 30;
const agentCpu = 1024;
const agentMemory = 2048;
const agentTaskCount = 1;
const OIDCOrgId = process.env['OIDC_ORG_ID'] || '';
const OIDCProjectId = process.env['OIDC_PROJECT_ID'] || '';
const gitSha = process.env['CIRCLE_SHA1'] || 'dev';
const imageTag = gitSha.slice(0, 7);
const vpcName = isDev ? 'SharedVpc' : 'Pocket VPC';
const pocketDataItemsBucket = isDev
  ? 'pocket-snowflake-dev'
  : 'pocket-data-items';

export const config = {
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
  OIDCOrgId,
  OIDCProjectId,
  imageTag,
  vpcName,
  pocketDataItemsBucket
};
