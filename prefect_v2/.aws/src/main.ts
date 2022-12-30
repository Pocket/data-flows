// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0
import { Construct } from 'constructs';
import { AwsProvider } from '@cdktf/provider-aws/lib/provider';
import { config } from './config';
import {
  App,
  TerraformStack,
  CloudBackend,
  NamedCloudWorkspace,
  Fn
} from 'cdktf';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { CloudwatchLogGroup } from '@cdktf/provider-aws/lib/cloudwatch-log-group';
import { EcsCluster } from '@cdktf/provider-aws/lib/ecs-cluster';
import { EcsClusterCapacityProviders } from '@cdktf/provider-aws/lib/ecs-cluster-capacity-providers';
import { AgentIamRoles, CircleCIDevRole } from './iam';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { EcsTaskDefinition } from '@cdktf/provider-aws/lib/ecs-task-definition';
import { EcsService } from '@cdktf/provider-aws/lib/ecs-service';
import { buildDefinitionJSON } from '@pocket/terraform-modules';
import { DataAwsSsmParameter } from '@cdktf/provider-aws/lib/data-aws-ssm-parameter';

class PrefectV2 extends TerraformStack {
  private readonly region: DataAwsRegion;
  private readonly logGroup: CloudwatchLogGroup;
  private readonly dockerSecret: DataAwsSecretsmanagerSecret;
  private readonly prefectV2Secret: DataAwsSecretsmanagerSecret;
  private readonly privateSubnets: DataAwsSsmParameter;
  private readonly ecsCluster: EcsCluster;
  private readonly agentRoles: AgentIamRoles;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    new AwsProvider(this, 'AWS', {
      region: config.region,
      defaultTags: {
        tags: config.tags
      },
      assumeRole:
        config.testCircleCIArn === undefined
          ? config.testCircleCIArn
          : {
              roleArn: config.testCircleCIArn,
              externalId: 'prefect-v2-circleci-local',
              sessionName: 'prefect-v2-circleci-local'
            }
    });

    this.privateSubnets = new DataAwsSsmParameter(this, `privateSubnets`, {
      name: '/Shared/PrivateSubnets'
    });

    this.region = new DataAwsRegion(this, 'region');
    const caller = new DataAwsCallerIdentity(this, 'caller');

    this.dockerSecret = new DataAwsSecretsmanagerSecret(this, 'dockerSecret', {
      name: 'Shared/DockerHub'
    });

    this.prefectV2Secret = new DataAwsSecretsmanagerSecret(
      this,
      'prefectV2Secret',
      {
        name: `dpt/${config.tags.environment}/prefect_v2`
      }
    );

    this.logGroup = new CloudwatchLogGroup(this, 'logGroup', {
      name: `prefect-v2-agent-log-group-${config.tags.environment}`,
      retentionInDays: config.log_retention_days
    });

    this.ecsCluster = new EcsCluster(this, 'ecsCluster', {
      name: `prefect-v2-${config.tags.environment}`
    });

    new EcsClusterCapacityProviders(this, 'ecsCapacityProvider', {
      clusterName: this.ecsCluster.name,
      capacityProviders: ['FARGATE']
    });

    this.agentRoles = new AgentIamRoles(
      this,
      'agentRoles',
      this.dockerSecret,
      this.prefectV2Secret,
      this.ecsCluster,
      caller
    );

    this.getAgentService('test');
    this.getAgentService('live');
  }
  private getAgentContainerDefinition(deploymentType: string): string {
    const containerDef = buildDefinitionJSON({
      name: `prefect-v2-agent-${config.tags.environment}-${deploymentType}`,
      containerImage: config.agentImage,
      repositoryCredentialsParam: this.dockerSecret.arn,
      command: [
        'prefect',
        'agent',
        'start',
        '-q',
        `prefect-v2-queue-${config.tags.environment}-${deploymentType}`
      ],
      secretEnvVars: [
        {
          name: 'PREFECT_API_KEY',
          valueFrom: `${this.prefectV2Secret.arn}:service_account_api_key::`
        },
        {
          name: 'PREFECT_API_URL',
          valueFrom: `${this.prefectV2Secret.arn}:account_workspace_url::`
        }
      ],
      logGroup: this.logGroup.name,
      logGroupRegion: this.region.name
    });
    return `[${containerDef}]`;
  }
  private getAgentService(deploymentType: string) {
    const DeploymentTypeProper =
      deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1);
    const agentTaskDef = new EcsTaskDefinition(
      this,
      `prefectV2AgentTask${DeploymentTypeProper}`,
      {
        family: `prefect-v2-agent-${config.tags.environment}-${deploymentType}`,
        cpu: config.agentCpu,
        memory: config.agentMemory,
        requiresCompatibilities: ['FARGATE'],
        networkMode: 'awsvpc',
        containerDefinitions: this.getAgentContainerDefinition(deploymentType),
        executionRoleArn: this.agentRoles.agentExecutionRole.arn,
        taskRoleArn: this.agentRoles.agentTaskRole.arn
      }
    );

    new EcsService(this, `prefectV2AgentService${DeploymentTypeProper}`, {
      name: `prefect-agent-v2-${config.tags.environment}-${deploymentType}`,
      cluster: this.ecsCluster.id,
      desiredCount: config.agentTaskCount,
      launchType: 'FARGATE',
      taskDefinition: agentTaskDef.id,
      networkConfiguration: {
        subnets: Fn.split(',', this.privateSubnets.value)
      }
    });
  }
}

class CircleCIDev extends TerraformStack {
  constructor(scope: Construct, id: string) {
    super(scope, id);

    new AwsProvider(this, 'AWS', {
      region: config.region,
      defaultTags: {
        tags: config.tags
      }
    });

    const region = new DataAwsRegion(this, 'region');
    const caller = new DataAwsCallerIdentity(this, 'caller');

    new CircleCIDevRole(this, 'circleCIDevRole', region, caller);
  }
}

const app = new App();
const prefectStack = new PrefectV2(app, 'prefect-v2');
new CloudBackend(prefectStack, {
  hostname: 'app.terraform.io',
  organization: 'Pocket',
  workspaces: new NamedCloudWorkspace(config.workspaceName)
});

if (config.isLocal) {
  const circleCIDevStack = new CircleCIDev(app, 'prefect-v2-circleci');
  new CloudBackend(circleCIDevStack, {
    hostname: 'app.terraform.io',
    organization: 'Pocket',
    workspaces: new NamedCloudWorkspace(config.circleCIDevWorkspaceName)
  });
}
app.synth();
