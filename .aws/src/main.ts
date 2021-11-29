import {Construct} from 'constructs';
import {App, Fn, RemoteBackend, TerraformStack,} from 'cdktf';
import {config} from './config';
import {PocketALBApplication, PocketECSCodePipeline, PocketVPC,} from '@pocket-tools/terraform-modules';

// Providers
import {
  AwsProvider,
  DataSources,
  KMS,
  SNS,
  S3,
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

    const pocketVPC = new PocketVPC(this, 'pocket-shared-vpc');

    const configBucket = this.createConfigBucket();
    const runTaskKwargsObject = this.createRunTaskKwargsObject(configBucket.id, pocketVPC);

    const pocketApp = this.createPocketAlbApplication({
      secretsManagerKmsAlias: this.getSecretsManagerKmsAlias(),
      snsTopic: this.getCodeDeploySnsTopic(),
      region,
      caller,
      configBucket,
      runTaskKwargsObject,
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

  /**
   * Create an S3 bucket for Prefect configuration objects.
   * @private
   */
  private createConfigBucket(): S3.S3Bucket {
    return new S3.S3Bucket(this, 'prefect-config-bucket', {
      bucket: `pocket-${config.name}-config-${config.environment}`.toLowerCase(),
      acl: 'private',
      forceDestroy: true, // Allow the bucket to be deleted even if it's not empty.
      lifecycle: {
        preventDestroy: false,
      },
      tags: config.tags,
    });
  }

  /**
   * Create configuration for Prefect's --run-task-kwargs argument, to control how tasks are started by Prefect in ECS.
   * @see https://docs.prefect.io/orchestration/agents/ecs.html#custom-runtime-options
   * @see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
   * @private
   */
  private createRunTaskKwargsObject(bucket: string, vpc: PocketVPC): S3.S3BucketObject {
    const runTaskKwargs = {
        networkConfiguration: {
            awsvpcConfiguration: {
                subnets: vpc.privateSubnetIds,
                securityGroups: vpc.defaultSecurityGroups.ids,
                assignPublicIp: 'DISABLED'
            }
        }
    };

    const content = Fn.yamlencode(runTaskKwargs);

    return new S3.S3BucketObject(this, 'run-task-kwargs-object', {
      bucket,
      key: 'run_task_kwargs.yml',
      content,
      etag: Fn.md5(content),
    });
  }

  private createPocketAlbApplication(dependencies: {
    region: DataSources.DataAwsRegion;
    caller: DataSources.DataAwsCallerIdentity;
    secretsManagerKmsAlias: KMS.DataAwsKmsAlias;
    snsTopic: SNS.DataAwsSnsTopic;
    configBucket: S3.S3Bucket;
    runTaskKwargsObject: S3.S3BucketObject;
  }): PocketALBApplication {
    const {
      region,
      caller,
      secretsManagerKmsAlias,
      snsTopic,
      configBucket,
      runTaskKwargsObject,
    } = dependencies;

    //Our shared dockerhub credentials in Secrets Manager to bypass docker hub pull limits
    const repositoryCredentials = `arn:aws:secretsmanager:${region.name}:${caller.accountId}:secret:Shared/DockerHub`;

    // Parameter store ARN prefix for this service.
    const parameterArnPrefix: string =
      `arn:aws:ssm:${region.name}:${caller.accountId}:parameter/${config.name}/${config.environment}`

    return new PocketALBApplication(this, 'application', {
      internal: true,
      prefix: config.prefix,
      alb6CharacterPrefix: config.shortName,
      tags: config.tags,
      cdn: false,
      domain: config.domain,
      containerConfigs: [
        {
          name: 'app',
          containerImage: 'prefecthq/prefect:0.15.9-python3.9',
          repositoryCredentialsParam: repositoryCredentials,
          portMappings: [
            {
              hostPort: config.prefect.port,
              containerPort: config.prefect.port,
            },
          ],
          // @see https://docs.prefect.io/orchestration/agents/ecs.html
          command: ['prefect', 'agent', 'ecs', 'start',
            '--cluster', config.name,  // ECS cluster to use for launching tasks
            '--launch-type', 'FARGATE',
            '--run-task-kwargs', `s3://${runTaskKwargsObject.bucket}/${runTaskKwargsObject.key}`,
            '--agent-address', `http://0.0.0.0:${config.prefect.port}`,  // run a HTTP server for use as a health check
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
          // Give read access to the configBucket, such that Prefect can load the config files from there.
          {
            actions: [
              's3:GetObject*',
              's3:ListBucket*',
            ],
            resources: [
              configBucket.arn,
              `${configBucket.arn}/*`,
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
