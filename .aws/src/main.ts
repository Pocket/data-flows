import {Construct} from 'constructs';
import {App, Fn, RemoteBackend, TerraformStack,} from 'cdktf';
import {config} from './config';
import {PocketALBApplication, PocketECSCodePipeline, PocketVPC,} from '@pocket-tools/terraform-modules';

// Providers
import {
  AwsProvider,
  DataSources,
  IAM,
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

    // Create a bucket with Prefect configuration.
    const configBucket = this.createConfigBucket();
    const runTaskKwargsObject = this.createRunTaskKwargsObject(configBucket.id, pocketVPC);

    // Create a bucket with Prefect storage.
    const storageBucket = this.createStorageBucket();

    // Create the Prefect agent in ECS.
    const pocketApp = this.createPocketAlbApplication({
      secretsManagerKmsAlias: this.getSecretsManagerKmsAlias(),
      snsTopic: this.getCodeDeploySnsTopic(),
      region,
      caller,
      configBucket,
      runTaskKwargsObject,
      storageBucket,
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
    // Set preventDestroy to false, because the contents of this bucket is generated through code on deployment.
    return this.createBucket('config', false);
  }

  /**
   * Create an S3 bucket Prefect storage
   *
   * After registration, the flow will be stored under <slugified-flow-name>/<slugified-current-timestamp>
   * Flows configured with S3 storage also default to using a S3Result for persisting any task results in this bucket.
   * @see https://docs.prefect.io/orchestration/flow_config/storage.html
   * @private
   */
  private createStorageBucket(): S3.S3Bucket {
    return this.createBucket('storage');
  }

  /**
   * Create an S3 bucket.
   * @param name
   * @param preventDestroy If true, the bucket is protected from being destroyed.
   * @private
   */
  private createBucket(name: string, preventDestroy: boolean = true): S3.S3Bucket {
    return new S3.S3Bucket(this, `prefect-${name.toLowerCase()}-bucket`, {
      bucket: `pocket-${config.name}-${name}-${config.environment}`.toLowerCase(),
      acl: 'private',
      forceDestroy: !preventDestroy, // Allow the bucket to be deleted even if it's not empty.
      lifecycle: {
        preventDestroy: preventDestroy,
      },
      tags: config.tags,
    });
  }

  /**
   * Returns a list of IAM policies that Prefect requires to start and manage tasks.
   * @private
   *
   * List of ECS operations, their resource type and conditions:
   * @see https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticcontainerservice.html
   */
  private getPrefectRunTaskPolicies(dependencies: {
    region: DataSources.DataAwsRegion;
    caller: DataSources.DataAwsCallerIdentity;
  }): IAM.DataAwsIamPolicyDocumentStatement[] {
    const {
      region,
      caller,
    } = dependencies;

    // This condition is added to operations to restrict them to the DataFlows ECS cluster.
    const DataFlowsClusterCondition = {
      test: 'ArnEquals',
      variable: 'ecs:cluster',
      values: [`arn:aws:ecs:${region.name}:${caller.accountId}:cluster/${config.prefix}`],
    };

    return [
      // The Prefect ECS Agent will need permissions to create task definitions and start tasks in your ECS Cluster.
      {
        actions: [
          'ecs:RunTask',
        ],
        resources: ['*'],
        //resources: [`arn:aws:ecs:${region.name}:${caller.accountId}:task-definition/prefect-*:*`],
        condition: [DataFlowsClusterCondition],
        effect: 'Allow',
      },
      {
        actions: [
          'ecs:StopTask',
        ],
        resources: ['*'],
        condition: [DataFlowsClusterCondition],
        effect: 'Allow',
      },
      // When Prefect runs tasks, it registers a task definition that it infers from the Prefect configuration.
      // Prefect only uses DeregisterTaskDefinition to clean up the task definition that Prefect creates:
      // @see https://github.com/PrefectHQ/prefect/search?q=deregister_task_definition
      //
      // The TaskDefinition ECS operations do not support resource-level permissions.
      // Policies granting access must specify "*" in the resource element.
      // https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticcontainerservice.html
      {
        actions: [
          'ecs:DeregisterTaskDefinition',
          'ecs:RegisterTaskDefinition',
          'ecs:DescribeTaskDefinition',
          'ecs:ListTaskDefinitions',
        ],
        resources: ['*'],
        effect: 'Allow',
      },
      // Prefect needs to be able to pass its role to the tasks it starts.
      {
        actions: [
          'iam:PassRole',
        ],
        resources: [`arn:aws:iam::${caller.accountId}:role/${config.prefix}-TaskRole`],
        effect: 'Allow',
      },
    ];
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
    storageBucket: S3.S3Bucket;
  }): PocketALBApplication {
    const {
      region,
      caller,
      secretsManagerKmsAlias,
      snsTopic,
      configBucket,
      runTaskKwargsObject,
      storageBucket,
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
          containerImage: 'prefecthq/prefect:0.15.10-python3.9',
          repositoryCredentialsParam: repositoryCredentials,
          portMappings: [
            {
              hostPort: config.prefect.port,
              containerPort: config.prefect.port,
            },
          ],
          // @see https://docs.prefect.io/orchestration/agents/ecs.html
          command: ['prefect', 'agent', 'ecs', 'start',
            '--cluster', config.prefix,  // ECS cluster to use for launching tasks
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
          ...this.getPrefectRunTaskPolicies({
            region,
            caller,
          }),
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
          // Give write access to the storageBucket, such that Prefect can load the Flow definition and save results.
          // @see https://docs.prefect.io/orchestration/flow_config/storage.html#pickle-vs-script-based-storage
          {
            actions: [
              's3:GetObject*',
              's3:PutObject*',
              's3:ListBucket*',
            ],
            resources: [
              storageBucket.arn,
              `${storageBucket.arn}/*`,
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
        targetMinCapacity: 1,
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
