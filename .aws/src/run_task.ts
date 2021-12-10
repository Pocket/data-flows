import {TerraformStack} from "cdktf";
import {Construct} from "constructs";
import {DataSources, IAM, S3} from "@cdktf/provider-aws";
import {config} from "./config";

export class RunTaskRole extends TerraformStack {
  public readonly iamRole: IAM.IamRole;

  constructor(scope: Construct, name: string) {
    super(scope, name);

    const region = new DataSources.DataAwsRegion(this, 'region');
    const caller = new DataSources.DataAwsCallerIdentity(this, 'caller');

    this.createRunTaskRole({
      region,
      caller,
    });
  }

  /**
   * Creates an IAM role for ECS tasks that execute the prefect task.
   * @private
   */
  private createRunTaskRole(dependencies: {
    region: DataSources.DataAwsRegion;
    caller: DataSources.DataAwsCallerIdentity;
  }): IAM.IamRole {
    const {
      region,
      caller,
    } = dependencies;

    const dataEcsTaskAssume = new IAM.DataAwsIamPolicyDocument(
      this,
      'run-task-assume',
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

    const runTaskRole = new IAM.IamRole(this, 'run-task-role', {
      assumeRolePolicy: dataEcsTaskAssume.json,
      name: `${config.prefix}-RunTaskRole`,
      tags: config.tags,
    });

    const s3Bucket = new S3.DataAwsS3Bucket(
      this,
      'pocket-data-learning-bucket',
      {bucket: 'pocket-data-learning'}
    );

    const dataEcsTaskRolePolicy = new IAM.DataAwsIamPolicyDocument(
      this,
      'data-ecs-task-role-policy',
      {
        version: '2012-10-17',
        statement: [{
          actions: [
            's3:GetObject*',
            's3:ListBucket*',
          ],
          resources: [
            s3Bucket.arn,
            `${s3Bucket.arn}/*`,
          ],
          effect: 'Allow',
        }],
      }
    );

    const ecsTaskRolePolicy = new IAM.IamPolicy(this, 'ecs-task-role-policy', {
      name: `${config.prefix}-TaskRolePolicy`,
      policy: dataEcsTaskRolePolicy.json,
    });

    new IAM.IamRolePolicyAttachment(this, 'ecs-task-custom-attachment', {
      policyArn: ecsTaskRolePolicy.arn,
      role: runTaskRole.id,
    });

    return runTaskRole;
  }
}
