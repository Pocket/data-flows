import { Resource } from 'cdktf';
import { Construct } from 'constructs';
import { datasources, iam, s3 } from '@cdktf/provider-aws';
import { config } from './config';
import { DataFlowsARN } from './DataFlowsARN';
import { ApplicationECSIAM } from './lib/ApplicationECSIAM';

export class DataFlowsEcsIam extends Resource {
  public readonly taskRole: iam.IamRole;
  public readonly executionRole: iam.IamRole;

  private readonly dataFlowsArn: DataFlowsARN;

  constructor(
    scope: Construct,
    name: string,
    private prefectStorageBucket: s3.S3Bucket
  ) {
    super(scope, name);

    const region = new datasources.DataAwsRegion(this, 'region');
    const caller = new datasources.DataAwsCallerIdentity(this, 'caller');

    this.dataFlowsArn = new DataFlowsARN(region, caller);

    // ApplicationECSIAM creates a task and an execution role.
    const ecsIamRoles = new ApplicationECSIAM(this, 'ecs-iam', {
      prefix: `${config.prefix}-Flow`,
      taskExecutionRolePolicyStatements: this.getExecutionRoleStatements(),
      taskRolePolicyStatements: this.getTaskRoleStatements(),
      tags: config.tags,
    });

    this.taskRole = ecsIamRoles.taskRole;
    this.executionRole = ecsIamRoles.taskExecutionRole;

    this.attachPoliciesToRole(
      this.getExistingPolicies(config.prefect.runTask.existingPolicies),
      this.taskRole
    );
  }

  /**
   * Get statements that will be attached to the task role, for running Prefect Flows.
   */
  private getTaskRoleStatements = () => [
    this.getDataLearningS3BucketReadAccess(),
    this.getStepFunctionExecuteAccess(),
    this.getPrefectStorageS3BucketWriteAccess(this.prefectStorageBucket),
    this.putFeatureGroupRecordsAccess(),
  ];

  /**
   * Get statements for the execution role, that's used to start the ECS task for running Prefect Flows.
   */
  private getExecutionRoleStatements = () => [
    this.getParameterStoreSecretAccess(),
  ];

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
   * Give Read access to s3 bucket pocket-data-learning (or pocket-data-learning-dev in Pocket-Dev).
   * @private
   */
  private getDataLearningS3BucketReadAccess(): iam.DataAwsIamPolicyDocumentStatement {
    const s3Bucket = new s3.DataAwsS3Bucket(
      this,
      'pocket-data-learning-bucket',
      { bucket: config.prefect.runTask.dataLearningBucketName }
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
   * Give write access to the storageBucket, such that Prefect can load the Flow definition and save results.
   * @see https://docs.prefect.io/orchestration/flow_config/storage.html#pickle-vs-script-based-storage
   * @private
   */
  private getPrefectStorageS3BucketWriteAccess(
    s3Bucket: s3.S3Bucket
  ): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:GetObject*', 's3:PutObject*', 's3:ListBucket*'],
      resources: [s3Bucket.arn, `${s3Bucket.arn}/*`],
      effect: 'Allow',
    };
  }

  /**
   * Give read access all DataFlows secrets in the Parameter Store
   * @private
   */
  private getParameterStoreSecretAccess(): iam.DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ssm:GetParameter*'],
      resources: [this.dataFlowsArn.getParameterStoreArn('*')],
      effect: 'Allow',
    };
  }

  private attachPoliciesToRole(
    policies: (iam.IamPolicy | iam.DataAwsIamPolicy)[],
    runTaskRole: iam.IamRole | iam.DataAwsIamRole
  ) {
    policies.forEach((policy) => {
      new iam.IamRolePolicyAttachment(this, policy.name.toLowerCase(), {
        policyArn: policy.arn,
        role: runTaskRole.id,
      });
    });
  }
}
