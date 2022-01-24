/**
 * This file is copied from Terraform-Modules. The only change is that the IAM roles are exposed.
 */
import { Resource } from 'cdktf';
import { Construct } from 'constructs';
import { iam } from '@cdktf/provider-aws';

export interface ApplicationECSIAMProps {
  prefix: string;
  taskExecutionRolePolicyStatements: iam.DataAwsIamPolicyDocumentStatement[];
  taskRolePolicyStatements: iam.DataAwsIamPolicyDocumentStatement[];
  taskExecutionDefaultAttachmentArn?: string;
  tags?: { [key: string]: string };
}

export class ApplicationECSIAM extends Resource {
  public readonly taskExecutionRoleArn;
  public readonly taskRoleArn;
  /**
   * Execution role is used during startup of the ECS task.
   */
  public readonly taskExecutionRole: iam.IamRole;
  /**
   * Task role is used for running the ECS task.
   */
  public readonly taskRole: iam.IamRole;

  constructor(scope: Construct, name: string, config: ApplicationECSIAMProps) {
    super(scope, name);

    // does anything here need to be in config?
    const dataEcsTaskAssume = new iam.DataAwsIamPolicyDocument(
      this,
      'ecs-task-assume',
      {
        version: '2012-10-17',
        statement: [
          {
            effect: 'Allow',
            actions: ['sts:AssumeRole'],
            principals: [
              {
                identifiers: ['ecs-tasks.amazonaws.com'],
                type: 'Service',
              },
            ],
          },
        ],
      }
    );

    this.taskExecutionRole = new iam.IamRole(this, 'ecs-execution-role', {
      assumeRolePolicy: dataEcsTaskAssume.json,
      name: `${config.prefix}-TaskExecutionRole`,
      tags: config.tags,
    });

    if (config.taskExecutionDefaultAttachmentArn) {
      new iam.IamRolePolicyAttachment(
        this,
        'ecs-task-execution-default-attachment',
        {
          policyArn: config.taskExecutionDefaultAttachmentArn,
          role: this.taskExecutionRole.id,
        }
      );
    }

    if (config.taskExecutionRolePolicyStatements.length > 0) {
      const dataEcsTaskExecutionRolePolicy = new iam.DataAwsIamPolicyDocument(
        this,
        'data-ecs-task-execution-role-policy',
        {
          version: '2012-10-17',
          statement: config.taskExecutionRolePolicyStatements,
        }
      );

      const ecsTaskExecutionRolePolicy = new iam.IamPolicy(
        this,
        'ecs-task-execution-role-policy',
        {
          name: `${config.prefix}-TaskExecutionRolePolicy`,
          policy: dataEcsTaskExecutionRolePolicy.json,
        }
      );

      new iam.IamRolePolicyAttachment(
        this,
        'ecs-task-execution-custom-attachment',
        {
          policyArn: ecsTaskExecutionRolePolicy.arn,
          role: this.taskExecutionRole.id,
        }
      );
    }

    this.taskRole = new iam.IamRole(this, 'ecs-task-role', {
      assumeRolePolicy: dataEcsTaskAssume.json,
      name: `${config.prefix}-TaskRole`,
      tags: config.tags,
    });

    if (config.taskRolePolicyStatements.length > 0) {
      const dataEcsTaskRolePolicy = new iam.DataAwsIamPolicyDocument(
        this,
        'data-ecs-task-role-policy',
        {
          version: '2012-10-17',
          statement: config.taskRolePolicyStatements,
        }
      );

      const ecsTaskRolePolicy = new iam.IamPolicy(
        this,
        'ecs-task-role-policy',
        {
          name: `${config.prefix}-TaskRolePolicy`,
          policy: dataEcsTaskRolePolicy.json,
        }
      );

      new iam.IamRolePolicyAttachment(this, 'ecs-task-custom-attachment', {
        policyArn: ecsTaskRolePolicy.arn,
        role: this.taskRole.id,
      });
    }

    // make arns available to other modules
    this.taskExecutionRoleArn = this.taskExecutionRole.arn;
    this.taskRoleArn = this.taskRole.arn;
  }
}
