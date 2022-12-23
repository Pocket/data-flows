import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { DataAwsIamPolicyDocument, DataAwsIamPolicyDocumentStatement, DataAwsIamPolicyDocumentStatementCondition } from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { EcsCluster } from '@cdktf/provider-aws/lib/ecs-cluster';
import { Construct } from 'constructs';
import { IamRole } from "@cdktf/provider-aws/lib/iam-role";
import { config } from './config';

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
          ]
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