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
  PocketVPC,
} from '@pocket-tools/terraform-modules';
import { config } from './config';
import { PocketECSCodePipeline } from './lib/PocketECSCodePipeline';
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
      vpcConfig: {
        vpcId: this.dependencies.pocketVPC.vpc.id,
        subnets: Fn.toset(this.dependencies.pocketVPC.privateSubnetIds),
        securityGroupIds: this.dependencies.pocketVPC.defaultSecurityGroups.ids,
      },
    });
  }

  private createFlowRegistrationIamRole() {
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

    const codeBuildRole = new iam.IamRole(this, 'codebuild_role', {
      name: `${config.shortName}-${config.environment}-FlowRegistrationRole`,
      assumeRolePolicy: dataCodebuildAssume.json,
      tags: config.tags,
    });

    // TODO: Limit permissions. It needs to run an ECS task and write to the Prefect S3 storage bucket.
    new iam.IamRolePolicyAttachment(this, 'codebuild_admin_policy_attachment', {
      policyArn: 'arn:aws:iam::aws:policy/AdministratorAccess',
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
