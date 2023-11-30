// Module to abstract IAM elements needed for Prefect v2
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import {
  DataAwsIamPolicyDocument,
  DataAwsIamPolicyDocumentStatement
} from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { Construct } from 'constructs';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { S3Bucket } from '@cdktf/provider-aws/lib/s3-bucket';
import { IamRole } from '@cdktf/provider-aws/lib/iam-role';
import { DataAwsIamOpenidConnectProvider } from '@cdktf/provider-aws/lib/data-aws-iam-openid-connect-provider';
import { config } from './config';
import { DataAwsS3Bucket } from '@cdktf/provider-aws/lib/data-aws-s3-bucket';

export class CircleCiOIDC extends Construct {
  constructor(
    scope: Construct,
    name: string,
    region: DataAwsRegion,
    caller: DataAwsCallerIdentity
  ) {
    super(scope, name);
    const orgId = config.OIDCOrgId;
    const OIDCProviderId = `oidc.circleci.com/org/${orgId}`;
    const OIDCProvider = new DataAwsIamOpenidConnectProvider(
      this,
      'OIDCProvider',
      {
        url: `https://${OIDCProviderId}`
      }
    );
    const trustPolicy = new DataAwsIamPolicyDocument(
      this,
      'DataFlowsOIDCTrust',
      {
        version: '2012-10-17',
        statement: [
          {
            effect: 'Allow',
            actions: ['sts:AssumeRoleWithWebIdentity', 'sts:TagSession'],
            principals: [
              {
                type: 'Federated',
                identifiers: [OIDCProvider.arn]
              }
            ],
            condition: [
              {
                test: 'StringEquals',
                variable: `${OIDCProviderId}:aud`,
                values: [orgId]
              },
              {
                test: 'ForAnyValue:StringLike',
                variable: `${OIDCProviderId}:sub`,
                values: [`org/${orgId}/project/${config.OIDCProjectId}/user/*`]
              }
            ]
          }
        ]
      }
    );
    const accessPolicy = new DataAwsIamPolicyDocument(
      this,
      'DataFlowsOIDCAccess',
      {
        version: '2012-10-17',
        statement: [
          {
            effect: 'Allow',
            actions: [
              'ecr:BatchCheckLayerAvailability',
              'ecr:GetRepositoryPolicy',
              'ecr:DescribeRepositories',
              'ecr:ListImages',
              'ecr:DescribeImages',
              'ecr:BatchGetImage',
              'ecr:InitiateLayerUpload',
              'ecr:UploadLayerPart',
              'ecr:CompleteLayerUpload',
              'ecr:PutImage'
            ],
            resources: [
              `arn:aws:ecr:${region.name}:${caller.accountId}:repository/data-flows-prefect-v2-envs`
            ]
          },
          {
            effect: 'Allow',
            actions: [
              'ecr:GetAuthorizationToken',
              'ecs:RegisterTaskDefinition',
              'ecs:ListTaskDefinitions',
              'ecs:DescribeTaskDefinition',
              'ecs:DeregisterTaskDefinition'
            ],
            resources: ['*']
          },
          {
            effect: 'Allow',
            actions: ['iam:PassRole'],
            resources: [
              `arn:aws:iam::${caller.accountId}:role/prefect-*`,
              `arn:aws:iam::${caller.accountId}:role/data-flows-*`
            ]
          }
        ]
      }
    );
    new IamRole(this, 'DataFlowsOIDCRole', {
      name: 'data-flows-ci-oidc-role',
      assumeRolePolicy: trustPolicy.json,
      inlinePolicy: [
        {
          name: 'data-flows-ci-oidc-policy',
          policy: accessPolicy.json
        }
      ]
    });
  }
}

// Custom construct to create IAM roles needed for a Prefect v2 Agent
export class AgentIamPolicies extends Construct {
  public readonly agentExecutionPolicyStatements: DataAwsIamPolicyDocumentStatement[];
  public readonly agentTaskPolicyStatements: DataAwsIamPolicyDocumentStatement[];
  private readonly ecsAppPrefix: string;
  private readonly caller: DataAwsCallerIdentity;
  private readonly region: DataAwsRegion;
  constructor(
    scope: Construct,
    name: string,
    prefectV2Secret: DataAwsSecretsmanagerSecret,
    dockerSharedSecret: DataAwsSecretsmanagerSecret,
    ecsAppPrefix: string,
    caller: DataAwsCallerIdentity,
    region: DataAwsRegion
  ) {
    super(scope, name);
    this.caller = caller;
    this.region = region;
    this.ecsAppPrefix = ecsAppPrefix;
    // create an inline policy doc for the execution role that can be combined with AWS managed policy
    this.agentExecutionPolicyStatements = [
      {
        actions: [
          'kms:Decrypt',
          'secretsmanager:GetSecretValue',
          'ssm:GetParameters'
        ],
        effect: 'Allow',
        resources: [prefectV2Secret.arn, dockerSharedSecret.arn]
      }
    ];

    // build the policy document for the Task role using Policy statement functions
    this.agentTaskPolicyStatements = [
      this.getAgentTaskAllAccess(),
      this.getAgentTaskEcsAccess(),
      this.getAgentTaskIamAccess(),
      this.getAgentTaskTagKeyBasedAccess()
    ];
  }
  // build policy statement for resources "*"
  private getAgentTaskAllAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'ecs:RegisterTaskDefinition',
        'ecs:ListTaskDefinitions',
        'ecs:DescribeTaskDefinition',
        'ecs:DeregisterTaskDefinition',
        'ec2:DescribeVpcs',
        'ec2:CreateNetworkInterface',
        'ec2:DescribeNetworkInterfaces',
        'ec2:AssignPrivateIpAddresses',
        'ec2:DescribeSubnets',
        'ecs:DescribeTasks'
      ],
      effect: 'Allow',
      resources: ['*']
    };
  }
  // build policy statement for TagKeys conditions
  private getAgentTaskTagKeyBasedAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ec2:DeleteNetworkInterface', 'ec2:UnassignPrivateIpAddresses'],
      effect: 'Allow',
      resources: ['*'],
      condition: [
        {
          test: 'StringLike',
          variable: 'aws:ResourceTag/prefect.io/flow-run-id',
          values: ['*']
        }
      ]
    };
  }
  // build policy statement for ECS task actions
  private getAgentTaskEcsAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ecs:StopTask', 'ecs:RunTask'],
      effect: 'Allow',
      resources: ['*'],
      condition: [
        {
          test: 'ArnLike',
          values: [
            `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/${this.ecsAppPrefix}*`
          ],
          variable: 'ecs:cluster'
        }
      ]
    };
  }
  // build policy statement that allows agent to pass the Prefect IAM roles
  private getAgentTaskIamAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['iam:PassRole'],
      effect: 'Allow',
      resources: [
        `arn:aws:iam::${this.caller.accountId}:role/${this.ecsAppPrefix}*`,
        `arn:aws:iam::${this.caller.accountId}:role/data-flows-*`
      ]
    };
  }
}

export class DataFlowsIamRoles extends Construct {
  private readonly fileSystem: S3Bucket;
  private readonly pocketDataItemBucket: DataAwsS3Bucket;
  private readonly caller: DataAwsCallerIdentity;
  private readonly region: DataAwsRegion;
  constructor(
    scope: Construct,
    name: string,
    fileSystem: S3Bucket,
    pocketDataItemBucket: DataAwsS3Bucket,
    caller: DataAwsCallerIdentity,
    region: DataAwsRegion,
    deploymentType: string
  ) {
    super(scope, name);
    this.caller = caller;
    this.region = region;
    this.fileSystem = fileSystem;
    this.pocketDataItemBucket = pocketDataItemBucket;
    // create an inline policy doc for the execution role that can be combined with AWS managed policy
    const flowExecutionPolicyStatements = [
      this.getSecrets(),
      {
        effect: 'Allow',
        actions: [
          'ecr:BatchCheckLayerAvailability',
          'ecr:GetRepositoryPolicy',
          'ecr:GetDownloadUrlForLayer',
          'ecr:DescribeRepositories',
          'ecr:ListImages',
          'ecr:DescribeImages',
          'ecr:BatchGetImage'
        ],
        resources: [
          `arn:aws:ecr:${region.name}:${caller.accountId}:repository/data-flows-prefect-v2-envs`
        ]
      },
      {
        effect: 'Allow',
        actions: [
          'ecr:GetAuthorizationToken',
          'logs:CreateLogStream',
          'logs:CreateLogGroup',
          'logs:PutLogEvents'
        ],
        resources: ['*']
      }
    ];

    // build the policy document for the Task role using Policy statement functions
    const flowTaskPolicyStatements = [
      this.getFlowS3BucketAccess(),
      this.getFlowS3ObjectAccess(),
      this.putFeatureGroupRecordsAccess(),
      this.getDataProductsSqsWriteAccess(),
      this.getSecrets()
    ];

    this.createFlowIamRole(
      `data-flows-prefect-${deploymentType}-exec-role`,
      flowExecutionPolicyStatements
    );
    this.createFlowIamRole(
      `data-flows-prefect-${deploymentType}-task-role`,
      flowTaskPolicyStatements
    );
  }
  // build policy statement for S3 bucket access
  private getFlowS3BucketAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:ListBucket'],
      effect: 'Allow',
      resources: [this.fileSystem.arn, this.pocketDataItemBucket.arn]
    };
  }
  // build policy statement for S3 object access
  private getFlowS3ObjectAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:*Object'],
      effect: 'Allow',
      resources: [
        `${this.fileSystem.arn}/*`,
        `${this.pocketDataItemBucket.arn}/*`
      ]
    };
  }
  // Give access to put records into a feature group
  private putFeatureGroupRecordsAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['sagemaker:PutRecord'],
      resources: ['arn:aws:sagemaker:*:*:feature-group/*'],
      effect: 'Allow'
    };
  }
  // Give SQS access to send messages.
  private getDataProductsSqsWriteAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['sqs:SendMessage', 'sqs:GetQueueUrl'],
      resources: [
        'arn:aws:sqs:*:*:RecommendationAPI-*',
        'arn:aws:sqs:*:*:ProspectAPI-*'
      ],
      effect: 'Allow'
    };
  }
  // Give access to secrets
  private getSecrets(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'kms:Decrypt',
        'secretsmanager:GetSecretValue',
        'ssm:GetParameters'
      ],
      effect: 'Allow',
      resources: [
        `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:dpt/${this.deploymentType}/data_flows_prefect_*`,
        `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:data-flows/${this.deploymentType}/*`
      ]
    };
  }
  // build policy statement for S3 object access
  private getFlowAssumeRoleAccess(role_name: string): DataAwsIamPolicyDocument {
    return new DataAwsIamPolicyDocument(this, `${role_name}TrustPolicy`, {
      version: '2012-10-17',
      statement: [
        {
          effect: 'Allow',
          actions: ['sts:AssumeRole'],
          principals: [
            {
              identifiers: ['ecs-tasks.amazonaws.com'],
              type: 'Service'
            }
          ]
        }
      ]
    });
  }
  private getFlowRolePolicy(
    role_name: string,
    policyStatements: DataAwsIamPolicyDocumentStatement[]
  ): DataAwsIamPolicyDocument {
    return new DataAwsIamPolicyDocument(this, `${role_name}AccessPolicy`, {
      version: '2012-10-17',
      statement: policyStatements
    });
  }
  private createFlowIamRole(
    name: string,
    policy: DataAwsIamPolicyDocumentStatement[]
  ): IamRole {
    const inline_policy = this.getFlowRolePolicy(name, policy);
    return new IamRole(this, name, {
      name: name,
      assumeRolePolicy: this.getFlowAssumeRoleAccess(name).json,
      inlinePolicy: [
        {
          name: `${name}-access-policy`,
          policy: inline_policy.json
        }
      ]
    });
  }
}
