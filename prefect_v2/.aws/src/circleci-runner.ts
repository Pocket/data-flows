// Module to abstract elements needed for a ircleCI Runner
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { EcsCluster } from '@cdktf/provider-aws/lib/ecs-cluster';
import { Construct } from 'constructs';
import { IamRole } from '@cdktf/provider-aws/lib/iam-role';
import { config } from './config';
import { buildDefinitionJSON } from '@pocket/terraform-modules';
import { Fn } from 'cdktf';
import { EcsTaskDefinition } from '@cdktf/provider-aws/lib/ecs-task-definition';
import { EcsService } from '@cdktf/provider-aws/lib/ecs-service';
import { DataAwsSsmParameter } from '@cdktf/provider-aws/lib/data-aws-ssm-parameter';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { CloudwatchLogGroup } from '@cdktf/provider-aws/lib/cloudwatch-log-group';

export class CircleCIRunner extends Construct {
  private readonly region: DataAwsRegion;
  private readonly logGroup: CloudwatchLogGroup;
  private readonly dockerSecret: DataAwsSecretsmanagerSecret;
  private readonly privateSubnets: DataAwsSsmParameter;
  private readonly ecsCluster: EcsCluster;
  private readonly runnerRole: IamRole;
  constructor(
    scope: Construct,
    name: string,
    runnerRole: IamRole,
    dockerSecret: DataAwsSecretsmanagerSecret,
    deploymentType: string
  ) {
    super(scope, name);

    this.dockerSecret = dockerSecret;
    this.runnerRole = runnerRole;
    this.region = new DataAwsRegion(this, 'region');

    // we need this for the runner service to deploy on private subnet
    this.privateSubnets = new DataAwsSsmParameter(this, `privateSubnets`, {
      name: '/Shared/PrivateSubnets'
    });

    // creates an ECS cluster for Self Hosted Runners
    this.ecsCluster = new EcsCluster(this, 'ecsCluster', {
      name: `circleci-runner-${config.tags.environment}`
    });

    // creates log group for the runner
    this.logGroup = new CloudwatchLogGroup(this, 'logGroup', {
      name: `circleci-runner-${config.tags.environment}`,
      retentionInDays: config.log_retention_days
    });

    // creates the CircleCI Runner
    this.getRunnerService(deploymentType);
  }
  // parametized container definition maker
  private getRunnerContainerDefinition(deploymentType: string): string {
    const runnerToken = new DataAwsSsmParameter(this, 'runnerToken', {
      name: '/SharedInfrastructure/DEFAULT_CIRCLECI_RUNNER_TOKEN'
    });

    const containerDef = buildDefinitionJSON({
      name: `circleci-runner-${config.tags.environment}-${deploymentType}`,
      containerImage: config.runnerImage,
      repositoryCredentialsParam: this.dockerSecret.arn,
      secretEnvVars: [
        {
          name: 'CIRCLECI_API_TOKEN',
          valueFrom: runnerToken.arn
        }
      ],
      envVars: [
        {
          name: 'CIRCLECI_RESOURCE_CLASS',
          value: 'pocket/default-dev'
        }
      ],
      logGroup: this.logGroup.name,
      logGroupRegion: this.region.name
    });
    return `[${containerDef}]`;
  }
  // create a task definition and service using private methods and params
  private getRunnerService(deploymentType: string) {
    const DeploymentTypeProper =
      deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1);
    const runnerTaskDef = new EcsTaskDefinition(
      this,
      `circleCIRunnerTask${DeploymentTypeProper}`,
      {
        family: `circleci-runner-${config.tags.environment}-${deploymentType}`,
        cpu: config.runnerCpu,
        memory: config.runnerMemory,
        requiresCompatibilities: ['FARGATE'],
        networkMode: 'awsvpc',
        containerDefinitions: this.getRunnerContainerDefinition(deploymentType),
        executionRoleArn: this.runnerRole.arn,
        taskRoleArn: this.runnerRole.arn
      }
    );

    new EcsService(this, `circleCIRunnerService${DeploymentTypeProper}`, {
      name: `circleci-runner-${config.tags.environment}-${deploymentType}`,
      cluster: this.ecsCluster.id,
      desiredCount: config.runnerTaskCount,
      launchType: 'FARGATE',
      taskDefinition: runnerTaskDef.id,
      networkConfiguration: {
        subnets: Fn.split(',', this.privateSubnets.value)
      }
    });
  }
}
