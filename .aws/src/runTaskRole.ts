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

    // Get all the policy statements that define the access that run tasks need.
    const statement = [
      this.getDataLearningS3BucketReadAccess(),
      this.getDataLearningStepFunctionExecuteAccess(),
      this.getPrefectStorageS3BucketWriteAccess(prefectStorageBucket),
    ];

    // Create a role with the above policy statement.
    this.iamRole = this.createRunTaskRole({
      statement,
      region,
      caller,
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
  private getDataLearningStepFunctionExecuteAccess(): IAM.DataAwsIamPolicyDocumentStatement {

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
  private createRunTaskRole(dependencies: {
    statement: IAM.DataAwsIamPolicyDocumentStatement[];
    region: DataSources.DataAwsRegion;
    caller: DataSources.DataAwsCallerIdentity;
  }): IAM.IamRole {
    const {
      statement,
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

    const dataEcsTaskRolePolicy = new IAM.DataAwsIamPolicyDocument(
      this,
      'data-run-task-role-policy',
      {
        version: '2012-10-17',
        statement,
      }
    );

    const ecsTaskRolePolicy = new IAM.IamPolicy(this, 'run-task-role-policy', {
      name: `${config.prefix}-TaskRolePolicy`,
      policy: dataEcsTaskRolePolicy.json,
    });

    new IAM.IamRolePolicyAttachment(this, 'run-task-custom-attachment', {
      policyArn: ecsTaskRolePolicy.arn,
      role: runTaskRole.id,
    });

    return runTaskRole;
  }
}
