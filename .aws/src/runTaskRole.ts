import {Resource} from "cdktf";
import {Construct} from "constructs";
import {DataSources, IAM, S3} from "@cdktf/provider-aws";
import {config} from "./config";

export class RunTaskRole extends Resource {
  public readonly iamRole: IAM.IamRole;

  constructor(scope: Construct, name: string, prefectStorageBucket: S3.S3Bucket) {
    super(scope, name);

    const region = new DataSources.DataAwsRegion(this, 'region');
    const caller = new DataSources.DataAwsCallerIdentity(this, 'caller');

    // Create a policy with all the additional access that run tasks need.
    const runTaskRolePolicy = this.createRunTaskRolePolicy([
      this.getDataLearningS3BucketReadAccess(),
      this.getStepFunctionExecuteAccess(),
      this.getPrefectStorageS3BucketWriteAccess(prefectStorageBucket),
    ]);

    // Get existing policies that run tasks need.
    const existingPolicies = this.getExistingPolicies(config.prefect.runTaskRole.existingPolicies);

    // Create a role with the above policies.
    this.iamRole = this.createRunTaskRole([
      ...existingPolicies,
      runTaskRolePolicy,
    ]);
  }

  /**
   * Return data sources for existing IAM policies.
   * @param names Existing policy names
   * @see https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy#path_prefix
   * @private
   */
  private getExistingPolicies(names: string[]): IAM.DataAwsIamPolicy[] {
    return names.map((name) => {
      return new IAM.DataAwsIamPolicy(this, 'pocket-data-product-read-only', {
        name: name,
      });
    });
  }

  /**
   * Create a policy
   * @param statement
   * @private
   */
  private createRunTaskRolePolicy(statement: IAM.DataAwsIamPolicyDocumentStatement[]): IAM.IamPolicy {
    const dataEcsTaskRolePolicy = new IAM.DataAwsIamPolicyDocument(
      this,
      'data-run-task-role-policy',
      {
        version: '2012-10-17',
        statement,
      }
    );

    return new IAM.IamPolicy(this, 'run-task-role-policy', {
      name: `${config.prefix}-RunTaskRolePolicy`,
      policy: dataEcsTaskRolePolicy.json,
    });
  }

  /**
   * Give Read access to S3 bucket pocket-data-learning (or pocket-data-learning-dev in Pocket-Dev).
   * @private
   */
  private getDataLearningS3BucketReadAccess(): IAM.DataAwsIamPolicyDocumentStatement {
    const s3Bucket = new S3.DataAwsS3Bucket(
      this,
      'pocket-data-learning-bucket',
      {bucket: config.prefect.runTaskRole.dataLearningBucketName}
    );

    return {
      actions: [
        's3:GetObject*',
        's3:ListBucket*',
        's3:HeadObject',
      ],
      resources: [
        s3Bucket.arn,
        `${s3Bucket.arn}/*`,
      ],
      effect: 'Allow',
    };
  }

  /**
   * Give access to trigger Step Functions for pocket-data-learning.
   * @private
   */
  private getStepFunctionExecuteAccess(): IAM.DataAwsIamPolicyDocumentStatement {
    return {
      actions: [ 'states:StartExecution' ],
      //TODO: Limit the resource to Metaflow step functions
      resources: [ 'arn:aws:states:*:*:stateMachine:*'],
      effect: 'Allow',
    };
  }

  /**
   * Give write access to the storageBucket, such that Prefect can load the Flow definition and save results.
   * @see https://docs.prefect.io/orchestration/flow_config/storage.html#pickle-vs-script-based-storage
   * @private
   */
  private getPrefectStorageS3BucketWriteAccess(s3Bucket: S3.S3Bucket): IAM.DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        's3:GetObject*',
        's3:PutObject*',
        's3:ListBucket*',
      ],
      resources: [
        s3Bucket.arn,
        `${s3Bucket.arn}/*`,
      ],
      effect: 'Allow',
    };
  }

  /**
   * Creates an IAM role for ECS tasks that execute the prefect task.
   * @private
   */
  private createRunTaskRole(policies: (IAM.IamPolicy | IAM.DataAwsIamPolicy)[]): IAM.IamRole {
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

    policies.forEach((policy) => {
      new IAM.IamRolePolicyAttachment(this, policy.name.toLowerCase(), {
        policyArn: policy.arn,
        role: runTaskRole.id,
      });
    });

    return runTaskRole;
  }
}
