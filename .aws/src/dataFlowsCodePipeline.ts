import { Fn, Resource } from 'cdktf';
import {
  codebuild,
  codepipeline,
  datasources,
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

export class DataFlowsCodePipeline extends Resource {
  private readonly pocketEcsCodePipeline: PocketECSCodePipeline;
  private readonly flowRegistrationCodeBuildProject: codebuild.CodebuildProject;

  constructor(
    scope: Construct,
    name: string,
    private dependencies: {
      region: datasources.DataAwsRegion;
      caller: datasources.DataAwsCallerIdentity;
      storageBucket: s3.S3Bucket;
      prefectAgentApp: PocketALBApplication;
      pocketVPC: PocketVPC;
    }
  ) {
    super(scope, name);

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
    // TODO: Get image ARN from PocketApp
    const imageTag = `dataflows-${config.environment.toLowerCase()}-app:latest`;
    const imageArn = `${this.dependencies.caller.accountId}.dkr.ecr.${this.dependencies.region.name}.amazonaws.com/${imageTag}`;

    const codeBuildRole = this.createFlowRegistrationIamRole(imageArn);

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
        image: imageArn,
        type: 'LINUX_CONTAINER',
        imagePullCredentialsType: 'SERVICE_ROLE',
      },
      source: {
        type: 'CODEPIPELINE',
        buildspec: 'buildspec.yml', // TODO: Use custom buildspec that registers Prefect flows.
      },
      vpcConfig: {
        vpcId: this.dependencies.pocketVPC.vpc.id,
        subnets: Fn.toset(this.dependencies.pocketVPC.privateSubnetIds),
        securityGroupIds: this.dependencies.pocketVPC.defaultSecurityGroups.ids,
      },
    });
  }

  private createFlowRegistrationIamRole(imageArn: string) {
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

    const policy = new iam.DataAwsIamPolicyDocument(
      this,
      'flow_registration_policy',
      {
        version: '2012-10-17',
        statement: [
          {
            actions: [
              'ecr:GetDownloadUrlForLayer',
              'ecr:BatchGetImage',
              'ecr:BatchCheckLayerAvailability',
            ],
            resources: [imageArn],
            effect: 'Allow',
          },
          {
            actions: [
              'codebuild:CreateReportGroup',
              'codebuild:CreateReport',
              'codebuild:UpdateReport',
              'codebuild:BatchPutTestCases',
            ],
            resources: ['*'],
            effect: 'Allow',
          },
        ],
      }
    );

    // TODO: Limit permissions. It needs to run an ECS task and write to the Prefect S3 storage bucket.
    new iam.IamRolePolicyAttachment(this, 'codebuild_admin_policy_attachment', {
      policyArn: 'arn:aws:iam::aws:policy/AdministratorAccess',
      role: codeBuildRole.name,
    });
    return codeBuildRole;
  }
}
