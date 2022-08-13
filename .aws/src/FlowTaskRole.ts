import { Resource } from 'cdktf';
import { Construct } from 'constructs';
import { iam, s3 } from '@cdktf/provider-aws';
import { config } from './config';

export class FlowTaskRole extends Resource {
  public readonly iamRole: iam.IamRole;

  constructor(scope: Construct, name: string, resultsBucket: s3.S3Bucket, athenaQueryOutputBucket: s3.S3Bucket) {
    super(scope, name);

    const existingPolicies = this.getExistingPolicies(
      config.prefect.flowTask.existingPolicies
    );

    // Create a policy with all the additional access that the ECS tasks need to execute Flows.
    const flowPolicy = this.createPolicy([
      this.getDataLearningS3BucketReadAccess(),
      this.getStepFunctionExecuteAccess(),
      this.getS3BucketWriteAccess(resultsBucket),
      this.getS3BucketWriteAccess(athenaQueryOutputBucket),
      this.athenaAccess(),
      this.putFeatureGroupRecordsAccess(),
      this.getDataProductsSqsWriteAccess(),
    ]);

    // Create a role with the above policies.
    this.iamRole = this.createRole([...existingPolicies, flowPolicy]);
  }

  /**
   * Return data sources for existing iam policies.
   * @param names Existing policy names
   * @see https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy#path_prefix
   * @private
   */
  private getExistingPolicies(names: string[]): iam.DataAwsIamPolicy[] {
    return names.map((name) => {
      return new iam.DataAwsIamPolicy(this, 'pocket-data-product-read-only', {
        name: name,
      });
    });
  }

  /**
   * Create a policy
   * @param statement
   * @private
   */
  private createPolicy(
    statement: iam.DataAwsIamPolicyDocumentStatement[]
  ): iam.IamPolicy {
    const dataEcsTaskRolePolicy = new iam.DataAwsIamPolicyDocument(
      this,
      'data-flow-task-role-policy',
      {
        version: '2012-10-17',
        statement,
      }
    );

    return new iam.IamPolicy(this, 'flow-task-role-policy', {
      name: `${config.prefix}-FlowTaskRolePolicy`,
      policy: dataEcsTaskRolePolicy.json,
    });
  }

  /**
   * Give Read access to s3 bucket pocket-data-learning (or pocket-data-learning-dev in Pocket-Dev).
   * @private
   */
  private getDataLearningS3BucketReadAccess(): iam.DataAwsIamPolicyDocumentStatement {
    const s3Bucket = new s3.DataAwsS3Bucket(
      this,
      'pocket-data-learning-bucket',
      { bucket: config.prefect.flowTask.dataLearningBucketName }
    );

    return {
      actions: ['s3:GetObject*', 's3:ListBucket*', 's3:HeadObject'],
      resources: [s3Bucket.arn, `${s3Bucket.arn}/*`],
      effect: 'Allow',
    };
  }

  /**
   * Give access to trigger Step Functions for pocket-data-learning.
   * @private
   */
  private getStepFunctionExecuteAccess(): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['states:StartExecution'],
      //TODO: Limit the resource to Metaflow step functions
      resources: ['arn:aws:states:*:*:stateMachine:*'],
      effect: 'Allow',
    };
  }

  /**
   * Give SQS access to send messages.
   * @private
   */
  private getDataProductsSqsWriteAccess(): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['sqs:SendMessage', 'sqs:GetQueueUrl'],
      resources: [
        'arn:aws:sqs:*:*:RecommendationAPI-*',
        'arn:aws:sqs:*:*:ProspectAPI-*',
      ],
      effect: 'Allow',
    };
  }

  /**
   * Give access to put records into a feature group
   * @private
   */
  private putFeatureGroupRecordsAccess(): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['sagemaker:PutRecord'],
      resources: ['arn:aws:sagemaker:*:*:feature-group/*'],
      effect: 'Allow',
    };
  }

  /**
   * Give access to query Athena
   * @private
   */
  private athenaAccess(): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'athena:GetQueryExecution',
        'athena:GetQueryResults',
        'athena:GetWorkGroup',
        'athena:ListDatabases',
        'athena:ListDataCatalogs',
        'athena:ListTableMetadata',
        'athena:StartQueryExecution',
        'athena:StopQueryExecution',
      ],
      resources: ['*'],
      effect: 'Allow',
    };
  }

  /**
   * Give write access to the Bucket.
   * @private
   */
  private getS3BucketWriteAccess(
    s3Bucket: s3.S3Bucket
  ): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:GetObject*', 's3:PutObject*', 's3:ListBucket*'],
      resources: [s3Bucket.arn, `${s3Bucket.arn}/*`],
      effect: 'Allow',
    };
  }

  /**
   * Creates an iam role for ECS tasks that execute the prefect task.
   * @private
   */
  private createRole(
    policies: (iam.IamPolicy | iam.DataAwsIamPolicy)[]
  ): iam.IamRole {
    const dataEcsTaskAssume = new iam.DataAwsIamPolicyDocument(
      this,
      'flow-task-assume',
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

    const role = new iam.IamRole(this, 'flow-task-role', {
      assumeRolePolicy: dataEcsTaskAssume.json,
      name: `${config.prefix}-FlowTaskRole`,
      tags: config.tags,
    });

    policies.forEach((policy) => {
      new iam.IamRolePolicyAttachment(this, policy.name.toLowerCase(), {
        policyArn: policy.arn,
        role: role.id,
      });
    });

    return role;
  }
}
