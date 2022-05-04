import { Resource } from 'cdktf';
import {
  codebuild,
  codepipeline,
  datasources,
  ecr,
  iam,
} from '@cdktf/provider-aws';
import { Construct } from 'constructs';
import { PocketECSCodePipeline } from '@pocket-tools/terraform-modules';
import { config } from './config';
import { DataFlowsARN } from './DataFlowsARN';

export class DataFlowsCodePipeline extends Resource {
  private readonly pocketEcsCodePipeline: PocketECSCodePipeline;
  private readonly flowRegistrationCodeBuildProject: codebuild.CodebuildProject;

  constructor(
    scope: Construct,
    name: string,
    private dependencies: {
      region: datasources.DataAwsRegion;
      caller: datasources.DataAwsCallerIdentity;
      flowTaskDefinitionArn: string;
      prefectImageRepository: ecr.EcrRepository;
      prefectImageRepositoryUri: string;
    }
  ) {
    super(scope, name);

    this.flowRegistrationCodeBuildProject =
      this.createFlowRegistrationCodeBuildProject();

    this.pocketEcsCodePipeline = this.createCodePipeline();
  }

  private createCodePipeline(): PocketECSCodePipeline {
    return new PocketECSCodePipeline(this, 'code-pipeline', {
      prefix: config.codePipeline.prefix,
      source: {
        codeStarConnectionArn: config.codePipeline.githubConnectionArn,
        repository: config.codePipeline.repository,
        branchName: config.codePipeline.branch,
      },
      codeDeploy: {
        applicationName: `${config.prefix}-ECS`,
        deploymentGroupName: `${config.prefix}-ECS`,
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
      buildTimeout: 60, // Timeout in minutes
      serviceRole: codeBuildRole.arn,
      artifacts: {
        type: 'CODEPIPELINE',
      },
      cache: { type: 'NO_CACHE' },
      environment: {
        computeType: 'BUILD_GENERAL1_SMALL',
        image: this.dependencies.prefectImageRepositoryUri,
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
            // Task Definition ARN for the ECS TaskRole to run flows.
            name: 'PREFECT_TASK_DEFINITION_ARN',
            value: this.dependencies.flowTaskDefinitionArn,
          },
          {
            // Environment variable for deployment.
            name: 'ENVIRONMENT',
            value: config.fullEnvironment,
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
    const sourceArtifactObjectArn = `arn:aws:s3:::pocket-codepipeline-*/${config.prefix}-*`;

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
            resources: [this.dependencies.prefectImageRepository.arn],
            effect: 'Allow',
          },
          {
            // Allow CodeBuild to inject the PREFECT_API_KEY from Parameter Store.
            actions: ['ssm:GetParameter*'],
            resources: [
              DataFlowsARN.getParameterArn(region, caller, 'PREFECT_API_KEY'),
            ],
            effect: 'Allow',
          },
          {
            // Allow CodeBuild to get the SourceOutput artifact from S3.
            actions: ['s3:GetObject'],
            resources: [sourceArtifactObjectArn],
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
      policyArn: policy.arn,
      role: codeBuildRole.name,
    });

    return codeBuildRole;
  }
}
