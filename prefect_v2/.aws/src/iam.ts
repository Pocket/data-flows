// Module to abstract IAM elements needed for Prefect v2
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import {
  DataAwsIamPolicyDocument,
  DataAwsIamPolicyDocumentStatement
} from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { EcsCluster } from '@cdktf/provider-aws/lib/ecs-cluster';
import { Construct } from 'constructs';
import { IamRole } from '@cdktf/provider-aws/lib/iam-role';
import { config } from './config';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { IamOpenidConnectProvider } from '@cdktf/provider-aws/lib/iam-openid-connect-provider';
import { DataAwsSecretsmanagerSecretVersion } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret-version';
import { Fn } from 'cdktf';
import { IamPolicy } from '@cdktf/provider-aws/lib/iam-policy';

// Both IAM Roles needed for Prefect v2 agent have the same trust policy
// Other task roles will have the same trust as well
// This Construct will provide re-usable base trust policy doc
export class BaseTrustPolicy extends Construct {
  public readonly baseTrustPolicy: DataAwsIamPolicyDocument;
  constructor(scope: Construct, name: string) {
    super(scope, name);
    const PolicyDoc = new DataAwsIamPolicyDocument(this, name, {
      version: '2012-10-17',
      statement: [
        {
          actions: ['sts:AssumeRole'],
          effect: 'Allow',
          principals: [
            {
              identifiers: ['ecs-tasks.amazonaws.com'],
              type: 'Service'
            }
          ]
        }
      ]
    });
    this.baseTrustPolicy = PolicyDoc;
  }
}

// Custom construct to create IAM roles needed for a Prefect v2 Agent
export class AgentIamRoles extends Construct {
  public readonly agentExecutionRole: IamRole;
  public readonly agentTaskRole: IamRole;
  private readonly ecsCluster: EcsCluster;
  constructor(
    scope: Construct,
    name: string,
    dockerSecret: DataAwsSecretsmanagerSecret,
    prefectV2Secret: DataAwsSecretsmanagerSecret,
    ecsCluster: EcsCluster,
    caller: DataAwsCallerIdentity
  ) {
    super(scope, name);
    this.ecsCluster = ecsCluster;
    // get the base Trust Policy Doc
    const baseTrustPolicy = new BaseTrustPolicy(this, 'baseTrustPolicy')
      .baseTrustPolicy;
    // create an inline policy doc for the execution role that can be combined with AWS managed policy
    const agentExecutionPolicy = new DataAwsIamPolicyDocument(
      this,
      'agentExecutionPolicy',
      {
        version: '2012-10-17',
        statement: [
          {
            actions: [
              'kms:Decrypt',
              'secretsmanager:GetSecretValue',
              'ssm:GetParameters'
            ],
            effect: 'Allow',
            resources: [dockerSecret.arn, prefectV2Secret.arn]
          }
        ]
      }
    );
    // create the execution role for Prefect v2 agent
    this.agentExecutionRole = new IamRole(this, 'agentExecutionRole', {
      name: `prefect-v2-agent-execution-role-${config.tags.environment}`,
      assumeRolePolicy: baseTrustPolicy.json,
      inlinePolicy: [
        {
          name: `prefect-v2-agent-execution-policy-${config.tags.environment}`,
          policy: agentExecutionPolicy.json
        }
      ],
      // combine inline with standard AWS ECS execution policy
      managedPolicyArns: [
        'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy'
      ]
    });
    // build the policy document for the Task role using Policy statement functions
    const agentTaskPolicy = new DataAwsIamPolicyDocument(
      this,
      'agentTaskPolicy',
      {
        version: '2012-10-17',
        statement: [
          this.getAgentTaskAllAccess(),
          this.getAgentTaskEcsAccess(),
          this.getAgentTaskIamAccess(caller),
          this.getAgentTaskXrayAccess()
        ]
      }
    );
    // create the task role for the Prefect v2 agent
    this.agentTaskRole = new IamRole(this, 'agentTaskRole', {
      name: `prefect-v2-agent-task-role-${config.tags.environment}`,
      assumeRolePolicy: baseTrustPolicy.json,
      inlinePolicy: [
        {
          name: `prefect-v2-agent-task-policy-${config.tags.environment}`,
          policy: agentTaskPolicy.json
        }
      ]
    });
  }
  // build policy statment for resources "*"
  private getAgentTaskAllAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'ecs:RegisterTaskDefinition',
        'ecs:ListTaskDefinitions',
        'ecs:DescribeTaskDefinition',
        'ecs:DeregisterTaskDefinition'
      ],
      effect: 'Allow',
      resources: ['*']
    };
  }
  // build policy statment for ECS task actions
  private getAgentTaskEcsAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ecs:StopTask', 'ecs:RunTask'],
      effect: 'Allow',
      resources: ['*'],
      condition: [
        {
          test: 'ArnEquals',
          values: [this.ecsCluster.arn],
          variable: 'ecs:cluster'
        }
      ]
    };
  }
  // build policy statement that allows agent to pass the Prefect IAM roles
  private getAgentTaskIamAccess(
    caller: DataAwsCallerIdentity
  ): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['iam:PassRole'],
      effect: 'Allow',
      resources: [`arn:aws:iam::${caller.accountId}:role/prefect-*`]
    };
  }
  // add xray access per Prefect v2 docs
  private getAgentTaskXrayAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'xray:PutTraceSegments',
        'xray:PutTelemetryRecords',
        'xray:GetSamplingTargets',
        'xray:GetSamplingStatisticSummaries',
        'xray:GetSamplingRules'
      ],
      effect: 'Allow',
      resources: ['*']
    };
  }
}

// Custom construct to create a OpenID Connect Role in the Dev Account
// This helps us test fine grained IAM Policies for CircleCI before applying in Production
export class CircleCIDevRole extends Construct {
  private readonly region: DataAwsRegion;
  private readonly caller: DataAwsCallerIdentity;
  constructor(
    scope: Construct,
    name: string,
    region: DataAwsRegion,
    caller: DataAwsCallerIdentity
  ) {
    super(scope, name);
    this.region = region;
    this.caller = caller;

    // We need this secret for the CircleCI OIDC ids and thumbprint
    const circleCIOIDCSecret = new DataAwsSecretsmanagerSecretVersion(
      this,
      'CircleCIOIDCSecret',
      {
        secretId: 'Shared/CircleCIOIDC'
      }
    );
    // create the OIDC Provder per CircleCI and AWS Docs
    const circleCIDevOIDCProvider = new IamOpenidConnectProvider(
      this,
      'circleCIDevOIDCProvider',
      {
        clientIdList: [
          Fn.lookup(
            Fn.jsondecode(circleCIOIDCSecret.secretString),
            'client_id',
            null
          )
        ],
        thumbprintList: [
          Fn.lookup(
            Fn.jsondecode(circleCIOIDCSecret.secretString),
            'thumbprint',
            null
          )
        ],
        url: Fn.join('', [
          'https://oidc.circleci.com/org/',
          Fn.lookup(
            Fn.jsondecode(circleCIOIDCSecret.secretString),
            'client_id',
            null
          )
        ])
      }
    );
    // Create a Trust that grants assume to CircleCI and the PocketSSODataLearning Role
    // This allows us to assume this role locally for testing Prefect-v2 stack with the CircleCI Policy
    const circleCIDevTrust = new DataAwsIamPolicyDocument(
      this,
      'circleCIDevTrust',
      {
        version: '2012-10-17',
        statement: [
          {
            actions: ['sts:AssumeRoleWithWebIdentity'],
            effect: 'Allow',
            principals: [
              {
                identifiers: [circleCIDevOIDCProvider.arn],
                type: 'Federated'
              }
            ],
            condition: [
              {
                test: 'StringEquals',
                values: [
                  Fn.lookup(
                    Fn.jsondecode(circleCIOIDCSecret.secretString),
                    'client_id',
                    null
                  )
                ],
                variable: Fn.format('oidc.circleci.com/org/%s:aud', [
                  Fn.lookup(
                    Fn.jsondecode(circleCIOIDCSecret.secretString),
                    'client_id',
                    null
                  )
                ])
              },
              {
                test: 'ForAnyValue:StringLike',
                values: [
                  Fn.format('org/%s/project/%s/user/*', [
                    Fn.lookup(
                      Fn.jsondecode(circleCIOIDCSecret.secretString),
                      'client_id',
                      null
                    ),
                    Fn.lookup(
                      Fn.jsondecode(circleCIOIDCSecret.secretString),
                      'project_id',
                      null
                    )
                  ])
                ],
                variable: Fn.format('oidc.circleci.com/org/%s:sub', [
                  Fn.lookup(
                    Fn.jsondecode(circleCIOIDCSecret.secretString),
                    'client_id',
                    null
                  )
                ])
              }
            ]
          },
          {
            actions: ['sts:AssumeRole'],
            effect: 'Allow',
            principals: [
              {
                identifiers: [
                  `arn:aws:iam::${this.caller.accountId}:role/PocketSSODataLearning`
                ],
                type: 'AWS'
              }
            ]
          }
        ]
      }
    );
    // Create the Policy doc for CircleCI with fine grained access to resources needed to deploy Prefect-v2 stack
    const circleCIDevPolicy = new DataAwsIamPolicyDocument(
      this,
      'circleCIDevPolicy',
      {
        version: '2012-10-17',
        statement: [
          this.getAllAccess(),
          this.getSecretsManagerAccess(),
          this.getIamAccess(),
          this.getEcsAccess(),
          this.getEcsTaskAccess()
        ]
      }
    );

    const circleCIDevManagedPolicy = new IamPolicy(
      this,
      'circleCIDevManagedPolicy',
      {
        name: 'CircleCIPrefectOpenIDPolicy',
        policy: circleCIDevPolicy.json
      }
    );

    new IamRole(this, 'circleCIDevRole', {
      name: 'CircleCIPrefectOpenIDRole',
      assumeRolePolicy: circleCIDevTrust.json,
      managedPolicyArns: [circleCIDevManagedPolicy.arn]
    });
  }
  private getAllAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:DescribeLogGroups',
        'logs:DescribeLogStreams',
        'logs:ListTags*',
        'ecs:CreateCluster',
        'ecs:DescribeCapacityProviders',
        'ecs:DescribeTaskDefinition'
      ],
      effect: 'Allow',
      resources: ['*']
    };
  }

  private getSecretsManagerAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'secretsmanager:Get*',
        'ssm:GetParameter',
        'secretsmanager:Describe*'
      ],
      effect: 'Allow',
      resources: [
        `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:Shared/*`,
        `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:dpt/*`,
        `arn:aws:ssm:${this.region.name}:${this.caller.accountId}:parameter/Shared/PrivateSubnets`
      ]
    };
  }
  private getIamAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'iam:Create*',
        'iam:Put*',
        'iam:Get*',
        'iam:Pass*',
        'iam:List*',
        'iam:Delete*',
        'iam:Attach*'
      ],
      effect: 'Allow',
      resources: [
        `arn:aws:iam::${this.caller.accountId}:role/prefect-*`,
        `arn:aws:iam::${this.caller.accountId}:policy/prefect-*`
      ]
    };
  }
  private getEcsAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ecs:Delete*', 'ecs:Put*', 'ecs:DescribeClusters'],
      effect: 'Allow',
      resources: [
        `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/prefect-*`,
        `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/dpt-*`
      ]
    };
  }
  private getEcsTaskAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'ecs:Start*',
        'ecs:Stop*',
        'ecs:Run*',
        'ecs:*Service*',
        'ecs:Describe*'
      ],
      effect: 'Allow',
      resources: ['*'],
      condition: [
        {
          test: 'ArnLike',
          values: [
            `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/prefect-*`,
            `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/dpt-*`
          ],
          variable: 'ecs:cluster'
        }
      ]
    };
  }
}
