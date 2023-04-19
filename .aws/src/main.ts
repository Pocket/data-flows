// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0
// Module to orchestrate our Terraform Stacks
import { Construct } from 'constructs';
import { AwsProvider } from '@cdktf/provider-aws/lib/provider';
import { config } from './config';
import { App, TerraformStack, CloudBackend, NamedCloudWorkspace } from 'cdktf';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { AgentIamPolicies, DataFlowsIamRoles, CircleCiOIDC } from './iam';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import {
  ApplicationECR,
  PocketECSApplication
} from '@pocket-tools/terraform-modules';
import { S3Bucket } from '@cdktf/provider-aws/lib/s3-bucket';
import { S3BucketVersioningA } from '@cdktf/provider-aws/lib/s3-bucket-versioning';
import { S3BucketPublicAccessBlock } from '@cdktf/provider-aws/lib/s3-bucket-public-access-block';
import { SecurityGroup } from '@cdktf/provider-aws/lib/security-group';
import { SecurityGroupRule } from '@cdktf/provider-aws/lib/security-group-rule';
import { DataAwsVpc } from '@cdktf/provider-aws/lib/data-aws-vpc';

// main Terraform Stack object for Prefect V2 infrastructure
class PrefectV2 extends TerraformStack {
  // these will enable access to variables in private methods
  private readonly region: DataAwsRegion;
  private readonly caller: DataAwsCallerIdentity;
  private readonly prefectV2Secret: DataAwsSecretsmanagerSecret;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    new AwsProvider(this, 'AWS', {
      region: config.region,
      defaultTags: {
        tags: config.tags
      }
    });

    // boiler plate for access to region and account id from iam creds
    this.region = new DataAwsRegion(this, 'region');
    this.caller = new DataAwsCallerIdentity(this, 'caller');
    // need this for the Prefect v2 API credentials
    this.prefectV2Secret = new DataAwsSecretsmanagerSecret(
      this,
      'prefectV2Secret',
      {
        name: `dpt/${config.tags.environment}/prefect_v2`
      }
    );
    // Custom construct to implement the IAM roles needed for the agent
    // Abstracting IAM stuff into iam.ts module

    // create new data-flows-prefect-envs ECR Repo
    const ecrRepo = new ApplicationECR(this, 'data-flows-prefect-envs-ecr', {
      name: 'data-flows-prefect-envs'
    });

    // we need an agent per queue
    // AWS dev will have the dev agent
    // AWS production will have the staging and main agent
    // we also need an s3 bucket per queue and set of iam roles
    // these map to github branch strategy of dev-v2, staging-v2, and main-v2
    if (config.isDev) {
      this.getAgentService('dev');
      const devS3Bucket = this.createDataFlowsBucket('dev');
      new DataFlowsIamRoles(
        this,
        'dataFlowDevRoles',
        devS3Bucket,
        this.caller,
        this.region,
        config.tags.environment,
        'dev',
        ecrRepo
      );
    } else {
      this.getAgentService('staging');
      const stagingS3Bucket = this.createDataFlowsBucket('staging');
      new DataFlowsIamRoles(
        this,
        'dataFlowStagingRoles',
        stagingS3Bucket,
        this.caller,
        this.region,
        config.tags.environment,
        'staging',
        ecrRepo
      );
      this.getAgentService('main');
      const mainS3Bucket = this.createDataFlowsBucket('main');
      new DataFlowsIamRoles(
        this,
        'dataFlowMainRoles',
        mainS3Bucket,
        this.caller,
        this.region,
        config.tags.environment,
        'main',
        ecrRepo
      );
    }

    // create the CircleCI OpenId Role for Image Upload
    new CircleCiOIDC(this, 'CircleCiOIDC', ecrRepo, this.caller);

    // create data-flows task security group

    const vpcId = new DataAwsVpc(this, 'vpcId', {
      tags: {
        Name: config.vpcName
      }
    });

    const baseDataFlowsSg = new SecurityGroup(this, 'BaseDataFlowsSg', {
      name: 'data-flows-prefect-base',
      vpcId: vpcId.id
    });

    new SecurityGroupRule(this, 'BaseDataFlowsSgEgress', {
      type: 'egress',
      fromPort: 0,
      toPort: 0,
      protocol: '-1',
      cidrBlocks: ['0.0.0.0/0'],
      securityGroupId: baseDataFlowsSg.id
    });
  }

  // create new data-flows-prefect-filesystem S3 buckets
  // this is used for flow artifacts and staging as needed
  private createDataFlowsBucket(deploymentType: string): S3Bucket {
    const artifactsBucket = new S3Bucket(
      this,
      `dataFlowsPrefectFs${deploymentType}`,
      {
        bucket: `data-flows-prefect-fs-${deploymentType}`
      }
    );
    new S3BucketVersioningA(
      this,
      `dataFlowsPrefectFsVConfig${deploymentType}`,
      {
        bucket: artifactsBucket.id,
        versioningConfiguration: {
          status: 'Enabled'
        }
      }
    );
    new S3BucketPublicAccessBlock(
      this,
      `dataFlowsPrefectFsPublicAccess${deploymentType}`,
      {
        bucket: artifactsBucket.id,
        blockPublicAcls: true,
        blockPublicPolicy: true,
        ignorePublicAcls: true,
        restrictPublicBuckets: true
      }
    );
    return artifactsBucket;
  }

  // create a task definition and service using private methods and params
  private getAgentService(deploymentType: string) {
    const prefix = `prefect-v2-agent-${deploymentType}`;
    const DeploymentTypeProper =
      deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1);

    const agentPolicies = new AgentIamPolicies(
      this,
      `agentPolicies${DeploymentTypeProper}`,
      this.prefectV2Secret,
      prefix,
      this.caller,
      this.region
    );

    // create the ECS Service for the Prefect v2 agent
    new PocketECSApplication(this, prefix, {
      prefix: prefix,
      shortName: `PFCTV2${deploymentType.toUpperCase()}`,
      taskSize: {
        cpu: config.agentCpu,
        memory: config.agentMemory
      },
      containerConfigs: [
        {
          name: `prefect-v2-agent`,
          containerImage: `${this.caller.accountId}.dkr.ecr.${this.region.name}.amazonaws.com/data-flows-prefect-envs:prefect-agent-${config.imageTag}`,
          command: [
            'prefect',
            'agent',
            'start',
            '-q',
            `prefect-v2-queue-${deploymentType}`
          ],
          secretEnvVars: [
            {
              name: 'PREFECT_API_KEY',
              valueFrom: `${this.prefectV2Secret.arn}:service_account_api_key::`
            },
            {
              name: 'PREFECT_API_URL',
              valueFrom: `${this.prefectV2Secret.arn}:account_workspace_url::`
            }
          ]
        }
      ],
      ecsIamConfig: {
        prefix: prefix,
        taskExecutionRolePolicyStatements:
          agentPolicies.agentExecutionPolicyStatements,
        taskRolePolicyStatements: agentPolicies.agentTaskPolicyStatements,
        taskExecutionDefaultAttachmentArn:
          'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy'
      }
    });
  }
}

// setup our App and add our stack(s)
const app = new App();
const prefectStack = new PrefectV2(app, 'prefect-v2');
new CloudBackend(prefectStack, {
  hostname: 'app.terraform.io',
  organization: 'Pocket',
  workspaces: new NamedCloudWorkspace(config.workspaceName)
});

app.synth();
