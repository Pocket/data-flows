import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { DataAwsIamPolicyDocument, DataAwsIamPolicyDocumentStatement, DataAwsIamPolicyDocumentStatementCondition } from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { EcsCluster } from '@cdktf/provider-aws/lib/ecs-cluster';
import { Construct } from 'constructs';
import { IamRole } from "@cdktf/provider-aws/lib/iam-role";
import { config } from './config';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { IamOpenidConnectProvider } from '@cdktf/provider-aws/lib/iam-openid-connect-provider';
import { DataAwsSecretsmanagerSecretVersion } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret-version';
import { Fn } from 'cdktf';
import { IamPolicy } from '@cdktf/provider-aws/lib/iam-policy';

export class BaseTrustPolicy extends Construct {
  public readonly baseTrustPolicy: DataAwsIamPolicyDocument;
  constructor(scope: Construct, name: string) {
      super(scope, name);
      const PolicyDoc = new DataAwsIamPolicyDocument(this, name, {
        version: "2012-10-17",
        statement: [
          {
            actions: ["sts:AssumeRole"],
            effect: "Allow",
            principals: [
            {
              identifiers: ["ecs-tasks.amazonaws.com"],
              type: "Service"
            }
          ]}]
    })
      this.baseTrustPolicy = PolicyDoc
    }
};

export class BaseExecutionPolicy extends Construct {
  public readonly baseExecutionPolicy: DataAwsIamPolicyDocument;
  constructor(scope: Construct, name: string, secrets: DataAwsSecretsmanagerSecret[]) {
      super(scope, name);
      var secretArns: string[] = new Array()
      secrets.forEach((secret) => {
        secretArns.push(secret.arn)
      });
      const PolicyDoc = new DataAwsIamPolicyDocument(this, name, {
        version: "2012-10-17",
        statement: [
          {
            actions: [
              "kms:Decrypt",
              "secretsmanager:GetSecretValue",
              "ssm:GetParameters"
            ],
            effect: "Allow",
            resources: secretArns
          }]
    })
      this.baseExecutionPolicy = PolicyDoc
    }
};

export class AgentIamRoles extends Construct {
  public readonly agentExecutionRole: IamRole;
  public readonly agentTaskRole: IamRole;
    constructor(scope: Construct, name: string, dockerSecret: DataAwsSecretsmanagerSecret, prefectV2Secret: DataAwsSecretsmanagerSecret, ecsCluster: EcsCluster, caller: DataAwsCallerIdentity) {
        super(scope, name);

        const baseTrustPolicy = new BaseTrustPolicy(this, "baseTrustPolicy").baseTrustPolicy
        const agentExecutionPolicy = new BaseExecutionPolicy(this, "agentExecutionPolicy", [
          dockerSecret,
          prefectV2Secret
        ])

        this.agentExecutionRole = new IamRole(this, 'agentExecutionRole', {
          name: `prefect-v2-agent-execution-role-${config.tags.environment}`,
          assumeRolePolicy: baseTrustPolicy.json,
          inlinePolicy: [
            {
              name: `prefect-v2-agent-execution-policy-${config.tags.environment}`,
              policy: agentExecutionPolicy.baseExecutionPolicy.json
            }
          ],
          managedPolicyArns: ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"]
        })

        const agentTaskPolicy = new DataAwsIamPolicyDocument(this, 'agentTaskPolicy', {
          version: "2012-10-17",
          statement: [
            this.getAgentTaskAllAccess(),
            this.getAgentTaskEcsAccess(this.getClusterCondition(ecsCluster)),
            this.getAgentTaskIamAccess(caller),
            this.getAgentTaskXrayAccess()
          ]
        })

        this.agentTaskRole = new IamRole(this, 'agentTaskRole', {
          name: `prefect-v2-agent-task-role-${config.tags.environment}`,
          assumeRolePolicy: baseTrustPolicy.json,
          inlinePolicy: [
            {
              name: `prefect-v2-agent-task-policy-${config.tags.environment}`,
              policy: agentTaskPolicy.json
            }
          ]
        })
    }

      private getAgentTaskAllAccess(): DataAwsIamPolicyDocumentStatement {
        return {
          actions: [
            "ecs:RegisterTaskDefinition",
            "ecs:ListTaskDefinitions",
            "ecs:DescribeTaskDefinition",
            "ecs:DeregisterTaskDefinition"
          ],
          effect: "Allow",
          resources: [
            "*"
          ]
        };
      };

      private getClusterCondition(ecsCluster: EcsCluster): DataAwsIamPolicyDocumentStatementCondition {
        return {
          test :"ArnEquals",
          values: [ecsCluster.arn],
          variable: "ecs:cluster"
        };
      };

      private getAgentTaskEcsAccess(clusterCondition: DataAwsIamPolicyDocumentStatementCondition): DataAwsIamPolicyDocumentStatement {
        return {
          actions: [
            "ecs:StopTask",
            "ecs:RunTask"
          ],
          effect: "Allow",
          resources: [
            "*"
          ],
          condition: [clusterCondition]
        }
      };

      private getAgentTaskIamAccess(caller: DataAwsCallerIdentity): DataAwsIamPolicyDocumentStatement {
        return {
          actions: [
            "iam:PassRole"
          ],
          effect: "Allow",
          resources: [
            `arn:aws:iam::${caller.accountId}:role/prefect-*`
          ]
        };
      };

      private getAgentTaskXrayAccess(): DataAwsIamPolicyDocumentStatement {
        return {
          actions: [
            "xray:PutTraceSegments",
            "xray:PutTelemetryRecords",
            "xray:GetSamplingTargets",
            "xray:GetSamplingStatisticSummaries",
            "xray:GetSamplingRules"
          ],
          effect: "Allow",
          resources: [
            "*"
          ]
        };
      };
  };

  export class CircleCIDevRole extends Construct {
    private readonly region: DataAwsRegion;
    private readonly caller: DataAwsCallerIdentity;
    constructor(scope: Construct, name: string, region: DataAwsRegion, caller: DataAwsCallerIdentity) {
      super(scope, name);
      this.region = region
      this.caller = caller

      const circleCIOIDCSecret = new DataAwsSecretsmanagerSecretVersion(this, 'CircleCIOIDCSecret', {
        secretId: "Shared/CircleCIOIDC"
      })

      const circleCIDevOIDCProvider = new IamOpenidConnectProvider(this, 'circleCIDevOIDCProvider', {
        clientIdList: [
          Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null)
        ],
        thumbprintList: [
          Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "thumbprint", null)
        ],
        url: Fn.join("", ["https://oidc.circleci.com/org/", Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null)])
      })

      const circleCIDevTrust = new DataAwsIamPolicyDocument(this, "circleCIDevTrust", {
        version: "2012-10-17",
          statement: [
            {
              actions: ["sts:AssumeRoleWithWebIdentity"],
              effect: "Allow",
              principals: [
                {
                  identifiers: [circleCIDevOIDCProvider.arn],
                  type: "Federated"
                }
              ],
              condition: [
                {
                  test: "StringEquals",
                  values: [Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null)],
                  variable: Fn.format("oidc.circleci.com/org/%s:aud", [Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null)])
                },
                {
                  test: "ForAnyValue:StringLike",
                  values: [
                    Fn.format("org/%s/project/%s/user/*", [
                      Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null), Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "project_id", null)
                    ])
                  ],
                  variable: Fn.format("oidc.circleci.com/org/%s:sub", [Fn.lookup(Fn.jsondecode(circleCIOIDCSecret.secretString), "client_id", null)])
                }
              ]
          },
          {
            actions: [
              "sts:AssumeRole"
            ],
            effect: "Allow",
            principals: [
              {
                identifiers: [
                  `arn:aws:iam::${this.caller.accountId}:role/PocketSSODataLearning`
                ],
                type: "AWS"
              }
            ]
          }
        ]
      })
      const circleCIDevPolicy = new DataAwsIamPolicyDocument(this, 'circleCIDevPolicy', {
        version: "2012-10-17",
          statement: [
            this.getAllAccess(),
            this.getSecretsManagerAccess(),
            this.getIamAccess(),
            this.getEcsAccess(),
            this.getEcsTaskAccess()
          ]
      })

      const circleCIDevManagedPolicy = new IamPolicy(this, 'circleCIDevManagedPolicy', {
        name: "CircleCIPrefectOpenIDPolicy",
        policy: circleCIDevPolicy.json
      })

      new IamRole(this, "circleCIDevRole", {
        name: "CircleCIPrefectOpenIDRole",
        assumeRolePolicy: circleCIDevTrust.json,
        managedPolicyArns: [
          circleCIDevManagedPolicy.arn
        ]
      })
    }
    private getAllAccess(): DataAwsIamPolicyDocumentStatement {
      return {
        actions: [
          "logs:Create*",
          "logs:Describe*",
          "logs:List*",
          "secretsmanager:Describe*",
          "ecs:CreateCluster",
          "ecs:Describe*",
          "ecs:RegisterTaskDefinition",
        ],
        effect: "Allow",
        resources: ["*"]
      }
    }
    private getSecretsManagerAccess(): DataAwsIamPolicyDocumentStatement {
      return {
        actions: [
          "secretsmanager:Get*",
          "ssm:GetParameter"
        ],
        effect: "Allow",
        resources: [
          `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:Shared/*`,
          `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:dpt/*`,
          `arn:aws:ssm:${this.region.name}:${this.caller.accountId}:parameter/Shared/PrivateSubnets`
      ]
      }
    }
    private getIamAccess(): DataAwsIamPolicyDocumentStatement {
      return {
        actions: [
          "iam:Create*",
          "iam:Put*",
          "iam:Get*",
          "iam:Pass*",
          "iam:List*",
          "iam:Delete*",
          "iam:Attach*"
        ],
        effect: "Allow",
        resources: [
          `arn:aws:iam::${this.caller.accountId}:role/prefect-*`, 
          `arn:aws:iam::${this.caller.accountId}:policy/prefect-*`
      ]
      }
    }
    private getEcsAccess(): DataAwsIamPolicyDocumentStatement {
      return {
        actions: [
          "ecs:Delete*",
          "ecs:Put*"
        ],
        effect: "Allow",
        resources: [
          `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/prefect-*`, 
          `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/dpt-*`
      ]
      }
    }
    private getEcsTaskAccess(): DataAwsIamPolicyDocumentStatement {
      return {
        actions: [
          "ecs:Start*",
          "ecs:Stop*",
          "ecs:Run*",
          "ecs:*Service*"
        ],
        effect: "Allow",
        resources: ["*"],
        condition: [
          {
            test :"ArnLike",
            values: [
              `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/prefect-*`,
              `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/dpt-*`
            ],
            variable: "ecs:cluster"
          }
        ]

      }
    }
  }