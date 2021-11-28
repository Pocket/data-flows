import {Construct} from 'constructs';
import {App, DataTerraformRemoteState, Fn, RemoteBackend, TerraformStack,} from 'cdktf';
import {config} from './config';
import {PocketALBApplication, PocketECSCodePipeline, PocketVPC,} from '@pocket-tools/terraform-modules';
import YAML from 'yaml'

// Providers
import {
  AwsProvider,
  ELB,
  ACM,
  DataSources,
  KMS,
  SNS,
} from '@cdktf/provider-aws';
import { LocalProvider } from '@cdktf/provider-local';
import { NullProvider } from '@cdktf/provider-null';

class DataFlows extends TerraformStack {
  constructor(scope: Construct, name: string) {
    super(scope, name);

    new AwsProvider(this, 'aws', { region: 'us-east-1' });
    new NullProvider(this, 'null', {});
    new LocalProvider(this, 'local', {});

    new RemoteBackend(this, {
      hostname: 'app.terraform.io',
      organization: 'Pocket',
      workspaces: [{ prefix: `${config.name}-` }],
    });

    const region = new DataSources.DataAwsRegion(this, 'region');
    const caller = new DataSources.DataAwsCallerIdentity(this, 'caller');

    const pocketApp = this.createPocketAlbApplication({
      secretsManagerKmsAlias: this.getSecretsManagerKmsAlias(),
      snsTopic: this.getCodeDeploySnsTopic(),
      region,
      caller,
    });

    this.createApplicationCodePipeline(pocketApp);
  }

  /**
   * Get the sns topic for code deploy
   * @private
   */
  private getCodeDeploySnsTopic() {
    return new SNS.DataAwsSnsTopic(this, 'backend_notifications', {
      name: `Backend-${config.environment}-ChatBot`,
    });
  }

  /**
   * Get secrets manager kms alias
   * @private
   */
  private getSecretsManagerKmsAlias() {
    return new KMS.DataAwsKmsAlias(this, 'kms_alias', {
      name: 'alias/aws/secretsmanager',
    });
  }

  /**
   * Create CodePipeline to build and deploy terraform and ecs
   * @param app
   * @private
   */
  private createApplicationCodePipeline(app: PocketALBApplication) {
    new PocketECSCodePipeline(this, 'code-pipeline', {
      prefix: config.prefix,
      source: {
        codeStarConnectionArn: config.codePipeline.githubConnectionArn,
        repository: config.codePipeline.repository,
        branchName: config.codePipeline.branch,
      },
    });
  }

  private createPocketAlbApplication(dependencies: {
    region: DataSources.DataAwsRegion;
    caller: DataSources.DataAwsCallerIdentity;
    secretsManagerKmsAlias: KMS.DataAwsKmsAlias;
    snsTopic: SNS.DataAwsSnsTopic;
  }): PocketALBApplication {
    const {
      region,
      caller,
      secretsManagerKmsAlias,
      snsTopic,
    } = dependencies;

    const parameterArnPrefix: string =
      `arn:aws:ssm:${region.name}:${caller.accountId}:parameter/${config.name}/${config.environment}`

    //Our shared dockerhub credentials in Secrets Manager to bypass docker hub pull limits
    const repositoryCredentials = `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:Shared/DockerHub`;

    const pocketVPC = new PocketVPC(this, 'pocket-shared-vpc');

    return new PocketALBApplication(this, 'application', {
      internal: true,
      prefix: config.prefix,
      alb6CharacterPrefix: config.shortName,
      tags: config.tags,
      cdn: false,
      domain: config.domain,
      containerConfigs: [
        {
          name: 'prefect',
          containerImage: 'prefecthq/prefect:0.15.9-python3.9',
          repositoryCredentialsParam: repositoryCredentials,
          portMappings: [
            {
              hostPort: config.prefect.port,
              containerPort: config.prefect.port,
            },
          ],
          command: ["prefect", "agent", "ecs", "start",
            "--cluster", config.name,  // ECS cluster to use for launching tasks
            "--launch-type", "FARGATE",  // Prefect launch type
            "--agent-address", `http://0.0.0.0:${config.prefect.port}`,  // run a HTTP server for use as a health check
          ],
          healthCheck: config.healthCheck,
          envVars: [
            {
              name: 'PREFECT__CLOUD__API',
              value: config.prefect.api,
            },
            {
              name: 'PREFECT__CLOUD__AGENT__LABELS',
              value: JSON.stringify(config.prefect.agentLabels),
            },
            {
              name: 'PREFECT__CLOUD__AGENT__LEVEL',
              value: config.prefect.agentLevel,
            },
            {
              // The subnets that Prefect should start tasks in.
              name: 'RUN_TASK_SUBNETS',
              // Fn.join needs to be used instead of the Javascript .join function, because the VPC properties aren't
              // known until `Terraform plan` is run.
              value: Fn.join(',', pocketVPC.privateSubnetIds),
            },
            {
              // The security groups that Prefect should start tasks with.
              name: 'RUN_TASK_SECURITY_GROUPS',
              // Fn.join needs to be used instead of the Javascript .join function, because the VPC properties aren't
              // known until `Terraform plan` is run.
              value: Fn.join(',', pocketVPC.defaultSecurityGroups.ids),
            },
          ],
          secretEnvVars: [
            {
              name: 'PREFECT__CLOUD__API_KEY',
              valueFrom: `${parameterArnPrefix}/PREFECT_API_KEY`,
            },
          ],
        },
      ],
      codeDeploy: {
        useCodeDeploy: true,
        useCodePipeline: true,
        snsNotificationTopicArn: snsTopic.arn,
      },
      exposedContainer: {
        name: 'prefect',
        port: config.prefect.port,
        healthCheckPath: '/api/health',
      },
      ecsIamConfig: {
        prefix: config.prefix,
        taskExecutionRolePolicyStatements: [
          //This policy could probably go in the shared module in the future.
          {
            actions: ['secretsmanager:GetSecretValue', 'kms:Decrypt'],
            resources: [
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:Shared`,
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:Shared/*`,
              secretsManagerKmsAlias.targetKeyArn,
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:${config.name}/${config.environment}`,
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:${config.name}/${config.environment}/*`,
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:${config.prefix}`,
              `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:${config.prefix}/*`,
            ],
            effect: 'Allow',
          },
          //This policy could probably go in the shared module in the future.
          {
            actions: ['ssm:GetParameter*'],
            resources: [
              parameterArnPrefix,
              `${parameterArnPrefix}/*`,
            ],
            effect: 'Allow',
          },
        ],
        taskRolePolicyStatements: [
          // The Prefect ECS Agent will need permissions to create task definitions and start tasks in your ECS Cluster.
          // @see https://docs.prefect.io/orchestration/agents/ecs.html#execution-role-arn
          {
            actions: [
                'ecs:RunTask',
                'ecs:StopTask',
                // 'ecs:CreateCluster',
                // 'ecs:DeleteCluster',
                'ecs:RegisterTaskDefinition',
                'ecs:DeregisterTaskDefinition',
                'ecs:DescribeClusters',
                'ecs:DescribeTaskDefinition',
                'ecs:DescribeTasks',
                'ecs:ListAccountSettings',
                'ecs:ListClusters',
                'ecs:ListTaskDefinitions',
            ],
            resources: [
              `arn:aws:ecs:${region.name}:${caller.accountId}:cluster/${config.prefix}`,
              `arn:aws:ecs:${region.name}:${caller.accountId}:task/${config.prefix}/*`,
              `arn:aws:ecs:${region.name}:${caller.accountId}:container/${config.prefix}/*`,
              `arn:aws:ecs:${region.name}:${caller.accountId}:task-definition/${config.prefix}:*`,
              `arn:aws:ecs:${region.name}:${caller.accountId}:container/${config.prefix}/*`,
            ],
            effect: 'Allow',
          },
          // X-Ray permissions
          {
            actions: [
              'xray:PutTraceSegments',
              'xray:PutTelemetryRecords',
              'xray:GetSamplingRules',
              'xray:GetSamplingTargets',
              'xray:GetSamplingStatisticSummaries',
            ],
            resources: ['*'],
            effect: 'Allow',
          },
        ],
        taskExecutionDefaultAttachmentArn:
          'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
      },
      autoscalingConfig: {
        targetMinCapacity: 2,
        targetMaxCapacity: 10,
      },
      alarms: {
        //TODO: When you start using the service add the pagerduty arns as an action `pagerDuty.snsNonCriticalAlarmTopic.arn`
        http5xxErrorPercentage: {
          threshold: 25,
          evaluationPeriods: 4,
          period: 300,
          actions: config.isDev ? [] : []
        }
      },
    });
  }
}

const app = new App();
new DataFlows(app, 'data-flows');
// TODO: Fix the terraform version. @See https://github.com/Pocket/related-content-api/pull/333
app.synth();
