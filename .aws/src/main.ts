import { Construct } from 'constructs';
import { App, Fn, RemoteBackend, TerraformStack } from 'cdktf';
import { config } from './config';
import {
  PocketALBApplication,
  PocketVPC,
} from '@pocket-tools/terraform-modules';

// Providers
import {
  AwsProvider,
  datasources,
  ecr,
  iam,
  kms,
  s3,
  sns,
} from '@cdktf/provider-aws';
import { LocalProvider } from '@cdktf/provider-local';
import { NullProvider } from '@cdktf/provider-null';
import {CodebuildProject} from "@cdktf/provider-aws/lib/codebuild";
import {IamRole, IamRolePolicy} from "@cdktf/provider-aws/lib/iam";

import { FlowTaskDefinition } from './FlowTaskDefinition';
import { FlowTaskRole } from './FlowTaskRole';
import { DataFlowsARN } from './DataFlowsARN';
import { DataFlowsCodePipeline } from './DataFlowsCodePipeline';

class DataFlows extends TerraformStack {
  private region: DataAwsRegion;
  private caller: DataAwsCallerIdentity;

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

    this.region = new datasources.DataAwsRegion(this, 'region');
    this.caller = new datasources.DataAwsCallerIdentity(this, 'caller');

    const pocketVPC = new PocketVPC(this, 'pocket-shared-vpc');

    // Create a bucket with Prefect configuration.
    const configBucket = this.createConfigBucket();
    const runTaskKwargsObject = this.createRunTaskKwargsObject(
      configBucket.id,
      pocketVPC
    );

    // Create a bucket with Prefect storage.
    const storageBucket = this.createStorageBucket();

    // Create task role for ECS tasks that execute flows.
    const flowTaskRole = new FlowTaskRole(
      this,
      'flow-task-role',
      storageBucket
    );

    // Create the Prefect agent in ECS.
    const prefectAgentApp = this.createPrefectAgentApp({
      secretsManagerKmsAlias: this.getSecretsManagerKmsAlias(),
      snsTopic: this.getCodeDeploySnsTopic(),
      region: this.region,
      caller: this.caller,
      configBucket,
      runTaskKwargsObject,
      flowTaskRole,
    });

    const ecrRepository = this.getPrefectEcrRepository(prefectAgentApp);
    const imageUri = `${ecrRepository.repositoryUrl}:latest`;

    // Create task definition for ECS tasks that execute flows.
    const flowTaskDefinition = new FlowTaskDefinition(this, 'ecs-flows', {
      region,
      caller,
      imageUri,
      taskRole: flowTaskRole.iamRole,
    });

    // Create a CodePipeline that deploys the Prefect Agent and registers the Prefect Flows with Prefect Cloud.
    new DataFlowsCodePipeline(this, 'data-flows-code-pipeline', {
      region: this.region,
      caller: this.caller,
      storageBucket,
      flowTaskDefinitionArn: flowTaskDefinition.taskDefinition.arn,
    });
  }

  /**
   * Gets the Prefect Agent ECR repository created by PocketALBApplication.
   * Terraform-Modules doesn't make this repository available, so we have to get it using DataAwsEcrRepository.
   * @param application
   * @private
   */
  private getPrefectEcrRepository(
    application: PocketALBApplication
  ): ecr.DataAwsEcrRepository {
    return new ecr.DataAwsEcrRepository(this, 'prefect-ecr-image', {
      name: `${config.prefix}-${config.prefect.agentContainerName}`.toLowerCase(),
      // The ECS repository is created in PocketALBApplication.ecsService, so we have a dependency on that.
      dependsOn: [application.ecsService],
    });

    this.getCodebuildLinter();
  }

  private getCodebuildLinter() {
    const role = new IamRole(this, 'linter-role', {
      name: `${config.prefix}-CodebuildLinterRole`,
      assumeRolePolicy: `
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "Service": "codebuild.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        }`
    });

    new IamRolePolicy(this, 'linter-role-policy', {
      role: role.name,
      policy: `
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Resource": [
                "*"
              ],
              "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
              ]
            },
             "Action": [
                "codebuild:CreateReportGroup",
                "codebuild:CreateReport",
                "codebuild:UpdateReport",
                "codebuild:BatchPutTestCases",
                "codebuild:BatchPutCodeCoverages"
            ],
            "Resource": "arn:aws:codebuild:${this.region}:${this.caller.accountId}:report-group/${config.prefix}*",
            "Effect": "Allow"
          ]
        }`
    });

    return new CodebuildProject(this, 'codebuild_linter', {
      name: `${config.prefix}-Linter`,
      serviceRole: role.name,

      artifacts: {
        type: "NO_ARTIFACS"
      },
      environment: {
        computeType: "BUILD_GENERAL1_SMALL",
        image: "aws/codebuild/standard:5.0",
        type: "LINUX_CONTAINER",
        imagePullCredentialsType: "CODEBUILD",
      },
      source: {
        type: 'GITHUB',
        location: 'https://github.com/Pocket/data-flows.git',
        buildspec: '', // TODO: Is this a buildspec file name or conents
      },
    });
  }

  /**
   * Get the sns topic for code deploy
   * @private
   */
  private getCodeDeploySnsTopic() {
    return new sns.DataAwsSnsTopic(this, 'data_products_notifications', {
      name: `DataAndLearning-${config.environment}-ChatBot`,
    });
  }

  /**
   * Get secrets manager kms alias
   * @private
   */
  private getSecretsManagerKmsAlias() {
    return new kms.DataAwsKmsAlias(this, 'kms_alias', {
      name: 'alias/aws/secretsmanager',
    });
  }

  /**
   * Create a s3 bucket for Prefect configuration objects.
   * @private
   */
  private createConfigBucket(): s3.S3Bucket {
    // Set preventDestroy to false, because the contents of this bucket is generated through code on deployment.
    return this.createBucket('config', false);
  }

  /**
   * Create an s3 bucket Prefect storage
   *
   * After registration, the flow will be stored under <slugified-flow-name>/<slugified-current-timestamp>
   * Flows configured with s3 storage also default to using a S3Result for persisting any task results in this bucket.
   * @see https://docs.prefect.io/orchestration/flow_config/storage.html
   * @private
   */
  private createStorageBucket(): s3.S3Bucket {
    return this.createBucket('storage');
  }

  /**
   * Create an s3 bucket.
   * @param name
   * @param preventDestroy If true, the bucket is protected from being destroyed.
   * @private
   */
  private createBucket(name: string, preventDestroy = true): s3.S3Bucket {
    return new s3.S3Bucket(this, `prefect-${name.toLowerCase()}-bucket`, {
      bucket:
        `pocket-${config.name}-${name}-${config.environment}`.toLowerCase(),
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
    region: datasources.DataAwsRegion;
    caller: datasources.DataAwsCallerIdentity;
    flowTaskRole: FlowTaskRole;
  }): iam.DataAwsIamPolicyDocumentStatement[] {
    const { region, caller, flowTaskRole } = dependencies;

    // This condition is added to operations to restrict them to the DataFlows ECS cluster.
    const DataFlowsClusterCondition = {
      test: 'ArnEquals',
      variable: 'ecs:cluster',
      values: [
        `arn:aws:ecs:${region.name}:${caller.accountId}:cluster/${config.prefix}`,
      ],
    };

    return [
      // The Prefect ECS Agent will need permissions to create task definitions and start tasks in your ECS Cluster.
      {
        actions: ['ecs:RunTask', 'ecs:StopTask'],
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
      // Prefect needs to be able to pass the execution role and task role to the tasks it starts.
      {
        actions: ['iam:PassRole'],
        resources: [
          DataFlowsARN.getFlowExecutionRoleArn(caller),
          flowTaskRole.iamRole.arn,
        ],
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
  private createRunTaskKwargsObject(
    bucket: string,
    vpc: PocketVPC
  ): s3.S3BucketObject {
    const runTaskKwargs = {
      networkConfiguration: {
        awsvpcConfiguration: {
          subnets: vpc.privateSubnetIds,
          securityGroups: vpc.defaultSecurityGroups.ids,
          assignPublicIp: 'DISABLED',
        },
      },
    };

    const content = Fn.yamlencode(runTaskKwargs);

    return new s3.S3BucketObject(this, 'run-task-kwargs-object', {
      bucket,
      key: 'run_task_kwargs.yml',
      content,
      etag: Fn.md5(content),
    });
  }

  private createPrefectAgentApp(dependencies: {
    region: datasources.DataAwsRegion;
    caller: datasources.DataAwsCallerIdentity;
    secretsManagerKmsAlias: kms.DataAwsKmsAlias;
    snsTopic: sns.DataAwsSnsTopic;
    configBucket: s3.S3Bucket;
    runTaskKwargsObject: s3.S3BucketObject;
    flowTaskRole: FlowTaskRole;
  }): PocketALBApplication {
    const {
      region,
      caller,
      secretsManagerKmsAlias,
      snsTopic,
      configBucket,
      runTaskKwargsObject,
      flowTaskRole,
    } = dependencies;
    return new PocketALBApplication(this, 'application', {
      internal: true,
      prefix: config.prefix,
      alb6CharacterPrefix: config.shortName,
      tags: config.tags,
      cdn: false,
      domain: config.domain,
      containerConfigs: [
        {
          name: config.prefect.agentContainerName,
          portMappings: [
            {
              hostPort: config.prefect.port,
              containerPort: config.prefect.port,
            },
          ],
          // @see https://docs.prefect.io/orchestration/agents/ecs.html
          command: [
            'prefect',
            'agent',
            'ecs',
            'start',
            '--cluster',
            config.prefix, // ECS cluster to use for launching tasks
            '--launch-type',
            'FARGATE',
            '--run-task-kwargs',
            `s3://${runTaskKwargsObject.bucket}/${runTaskKwargsObject.key}`,
            '--agent-address',
            `http://0.0.0.0:${config.prefect.port}`, // run a HTTP server for use as a health check
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
              valueFrom: DataFlowsARN.getParameterArn(
                region,
                caller,
                'PREFECT_API_KEY'
              ),
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
        name: 'app',
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
          {
            actions: ['ssm:GetParameter*'],
            resources: [DataFlowsARN.getParameterArn(region, caller, '*')],
            effect: 'Allow',
          },
        ],
        taskRolePolicyStatements: [
          ...this.getPrefectRunTaskPolicies({
            region,
            caller,
            flowTaskRole,
          }),
          // Give read access to the configBucket, such that Prefect can load the config files from there.
          {
            actions: ['s3:GetObject*', 's3:ListBucket*'],
            resources: [configBucket.arn, `${configBucket.arn}/*`],
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
        //TODO: When you start using the service add the pagerduty ARNs as an action `pagerDuty.snsNonCriticalAlarmTopic.arn`
        http5xxErrorPercentage: {
          threshold: 25,
          evaluationPeriods: 4,
          period: 300,
          actions: config.isDev ? [] : [],
        },
      },
    });
  }
}

const app = new App();
new DataFlows(app, 'data-flows');
// TODO: Fix the terraform version. @See https://github.com/Pocket/related-content-api/pull/333
app.synth();
