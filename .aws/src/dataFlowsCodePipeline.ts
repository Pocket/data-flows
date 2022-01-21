import { Fn, Resource } from 'cdktf';
import {
  codebuild,
  codepipeline,
  datasources,
  ecr,
  iam,
  s3,
} from '@cdktf/provider-aws';
import { Construct } from 'constructs';
import {
  PocketALBApplication,
  PocketECSCodePipeline,
  PocketVPC,
} from '@pocket-tools/terraform-modules';
import { config } from './config';
import {RunTaskRole} from "./runTaskRole";

export class DataFlowsCodePipeline extends Resource {
  private readonly pocketEcsCodePipeline: PocketECSCodePipeline;
  private readonly flowRegistrationCodeBuildProject: codebuild.CodebuildProject;
  private readonly prefectImageRepository: ecr.DataAwsEcrRepository;
  private readonly prefectImageUri: string;

  constructor(
    scope: Construct,
    name: string,
    private dependencies: {
      region: datasources.DataAwsRegion;
      caller: datasources.DataAwsCallerIdentity;
      storageBucket: s3.S3Bucket;
      prefectAgentApp: PocketALBApplication;
      runTaskRole: RunTaskRole,
      pocketVPC: PocketVPC;
    }
  ) {
    super(scope, name);

    this.prefectImageRepository = this.getPrefectEcrRepository();
    this.prefectImageUri = `${this.prefectImageRepository.repositoryUrl}:latest`;

    this.flowRegistrationCodeBuildProject =
      this.createFlowRegistrationCodeBuildProject();

    this.pocketEcsCodePipeline = this.createCodePipeline();
  }

  private createCodePipeline(): PocketECSCodePipeline {
    return new PocketECSCodePipeline(this, 'code-pipeline', {
      prefix: config.prefix,
      source: {
        codeStarConnectionArn: config.codePipeline.githubConnectionArn,
        repository: config.codePipeline.repository,
        branchName: config.codePipeline.branch,
      },
      postDeployStages: [
        {
          name: 'Register_Prefect_Flows',
          action: [
            this.getDeployPrefectCloudAction(
              this.flowRegistrationCodeBuildProject
            ),
          ],
        },
      ],
    });
  }

  /**
   * CodePipeline action to register Prefect flows using CodeBuild.
   * https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/codepipeline
   */
  private getDeployPrefectCloudAction = (
    prefectCloudDeployCodebuild: codebuild.CodebuildProject
  ): codepipeline.CodepipelineStageAction => ({
    name: 'Register_Prefect_Flows',
    category: 'Build',
    owner: 'AWS',
    provider: 'CodeBuild',
    version: '1',
    inputArtifacts: ['SourceOutput'],
    configuration: {
      ProjectName: prefectCloudDeployCodebuild.name,
    },
    runOrder: 1,
  });

  private createFlowRegistrationCodeBuildProject(): codebuild.CodebuildProject {
    const codeBuildRole = this.createFlowRegistrationIamRole();

    return new codebuild.CodebuildProject(this, 'deploy-prefect-codebuild', {
      name: `${config.prefix}-PrefectRegistration`,
      description: 'Registers Prefect flows with Prefect Cloud',
      buildTimeout: 60, // Timeout in hours
      serviceRole: codeBuildRole.arn,
      artifacts: {
        type: 'CODEPIPELINE',
      },
      cache: { type: 'NO_CACHE' },
      environment: {
        computeType: 'BUILD_GENERAL1_SMALL',
        image: this.prefectImageUri,
        type: 'LINUX_CONTAINER',
        imagePullCredentialsType: 'SERVICE_ROLE',
        environmentVariable: [
          {
            // Parameter store parameter name that holds the Prefect API key. CodeBuild will securely load this secret.
            name: 'PREFECT_APIKEY_PARAMETER_NAME',
            value: `/${config.name}/${config.environment}/PREFECT_API_KEY`,
          },
          {
            name: 'PREFECT_PROJECT_NAME',
            value: config.prefect.projectName,
          },
          {
            // S3 Storage bucket where the flows will be stored.
            name: 'PREFECT_STORAGE_BUCKET',
            value: this.dependencies.storageBucket.bucket,
          },
          {
            // S3 Storage bucket where the flows will be stored.
            name: 'PREFECT_IMAGE',
            value: this.prefectImageUri,
          },
          {
            // IAM Role ARN for the ECS TaskRole to run flows.
            name: 'PREFECT_RUN_TASK_ROLE',
            value: this.dependencies.runTaskRole.iamRole.arn,
          },
        ],
      },
      source: {
        type: 'CODEPIPELINE',
        buildspec: 'buildspec_register_flows.yml',
      },
    });
  }

  private createFlowRegistrationIamRole() {
    const region = this.dependencies.region;
    const caller = this.dependencies.caller;
    // This matches the SourceOutput S3 object ARN. I believe the bucket is created by CodePipeline, but we could
    // specify one ourselves in Terraform-Modules. I couldn't find how to get this value dynamically so I'm using `-*`.
    const sourceArtifactObjectArn = `arn:aws:s3:::pocket-codepipeline-*/${config.prefix}-CodePi/*`;

    const dataCodebuildAssume = new iam.DataAwsIamPolicyDocument(
      this,
      'flow_registration_codebuild_assume_role',
      {
        statement: [
          {
            actions: ['sts:AssumeRole'],
            effect: 'Allow',
            principals: [
              {
                identifiers: ['codebuild.amazonaws.com'],
                type: 'Service',
              },
            ],
          },
        ],
      }
    );

    const dataPolicy = new iam.DataAwsIamPolicyDocument(
      this,
      'flow_registration_policy_document',
      {
        version: '2012-10-17',
        statement: [
          {
            // Allow CodeBuild to log to CloudWatch.
            actions: [
              'logs:CreateLogGroup',
              'logs:CreateLogStream',
              'logs:PutLogEvents',
            ],
            resources: [
              `arn:aws:logs:${region.name}:${caller.accountId}:log-group:/aws/codebuild/*`,
            ],
            effect: 'Allow',
          },
          {
            // GetAuthorizationToken grants permission to get a temporary token to access the ECR repository below.
            // It cannot be limited to a particular resource:
            // https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticcontainerregistry.html
            actions: ['ecr:GetAuthorizationToken'],
            resources: ['*'],
            effect: 'Allow',
          },
          {
            // I copied these actions from an AWS CodeBuild example:
            // https://docs.aws.amazon.com/codebuild/latest/userguide/sample-ecr.html
            actions: [
              'ecr:GetDownloadUrlForLayer',
              'ecr:BatchGetImage',
              'ecr:BatchCheckLayerAvailability',
            ],
            resources: [this.prefectImageRepository.arn],
            effect: 'Allow',
          },
          {
            // Allow CodeBuild to inject the PREFECT_API_KEY from Parameter Store.
            actions: ['ssm:GetParameter*'],
            resources: [
              `arn:aws:ssm:${region.name}:${caller.accountId}:parameter/${config.name}/${config.environment}/PREFECT_API_KEY`,
            ],
            effect: 'Allow',
          },
          {
            // Allow CodeBuild to get the SourceOutput artifact from S3.
            actions: ['s3:GetObject'],
            resources: [sourceArtifactObjectArn],
            effect: 'Allow',
          },
          {
            // Allow CodeBuild to write to the Prefect Storage bucket.
            actions: [
              's3:PutObject',
            ],
            resources: [
              `${this.dependencies.storageBucket.arn}/*`,
            ],
            effect: 'Allow',
          },
        ],
      }
    );

    const policy = new iam.IamPolicy(this, 'flow_registration_policy', {
      name: `${config.prefix}-RegistrationPolicy`,
      policy: dataPolicy.json,
    });

    const codeBuildRole = new iam.IamRole(this, 'codebuild_role', {
      name: `${config.shortName}-${config.environment}-FlowRegistrationRole`,
      assumeRolePolicy: dataCodebuildAssume.json,
      tags: config.tags,
    });

    new iam.IamRolePolicyAttachment(this, 'codebuild_admin_policy_attachment', {
      // policyArn: 'arn:aws:iam::aws:policy/AdministratorAccess',
      policyArn: policy.arn,
      role: codeBuildRole.name,
    });

    return codeBuildRole;
  }

  private getPrefectEcrRepository(): ecr.DataAwsEcrRepository {
    return new ecr.DataAwsEcrRepository(this, 'prefect-ecr-image', {
      // TODO: If Terraform-Modules would expose the ECR repository that it creates, we could reference the repo name.
      name: `${config.prefix}-${config.prefect.agentContainerName}`.toLowerCase(),
    });
  }
}
