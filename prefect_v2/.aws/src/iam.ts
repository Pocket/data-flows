// Module to abstract IAM elements needed for Prefect v2
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { DataAwsIamPolicyDocumentStatement } from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { Construct } from 'constructs';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';

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
    dockerSecret: DataAwsSecretsmanagerSecret,
    prefectV2Secret: DataAwsSecretsmanagerSecret,
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
        resources: [dockerSecret.arn, prefectV2Secret.arn]
      }
    ];

    // build the policy document for the Task role using Policy statement functions
    this.agentTaskPolicyStatements = [
      this.getAgentTaskAllAccess(),
      this.getAgentTaskEcsAccess(),
      this.getAgentTaskIamAccess()
    ];
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
        `arn:aws:iam::${this.caller.accountId}:role/${this.ecsAppPrefix}*`
      ]
    };
  }
}
