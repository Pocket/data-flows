// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0
// Module to orchestrate our Terraform Stacks
import { Construct } from 'constructs';
import { AwsProvider } from '@cdktf/provider-aws/lib/provider';
import { config } from './config';
import { App, TerraformStack, CloudBackend, NamedCloudWorkspace } from 'cdktf';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import { AgentIamPolicies, DataFlowsIamRoles } from './iam';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import {
  ApplicationECR,
  PocketECSApplication
} from '@pocket-tools/terraform-modules';
import { S3Bucket } from '@cdktf/provider-aws/lib/s3-bucket';
import { S3BucketVersioningA } from '@cdktf/provider-aws/lib/s3-bucket-versioning';
import { S3BucketPublicAccessBlock } from '@cdktf/provider-aws/lib/s3-bucket-public-access-block';

// main Terraform Stack object for Prefect V2 infrastructure
class PrefectV2 extends TerraformStack {
  // these will enable access to variables in private methods
  private readonly region: DataAwsRegion;
  private readonly caller: DataAwsCallerIdentity;
  private readonly dockerSecret: DataAwsSecretsmanagerSecret;
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
    // need this bypass pull limits on DockerHub by logging into Pocket docker account
    this.dockerSecret = new DataAwsSecretsmanagerSecret(this, 'dockerSecret', {
      name: 'Shared/DockerHub'
    });
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

    // we need an agent per queue...so we create 2 agent services
    this.getAgentService('test');
    this.getAgentService('live');

    // create new data-flows-prefect-envs ECR Repo
    new ApplicationECR(this, 'data-flows-prefect-envs-ecr', {
      name: 'data-flows-prefect-envs'
    });

    // create live and test filesystems
    const testFS = this.createDataFlowsBucket('test');
    const liveFS = this.createDataFlowsBucket('live');

    // create live and test iam roles
    new DataFlowsIamRoles(
      this,
      'dataFlowTestRoles',
      testFS,
      this.caller,
      this.region,
      config.tags.environment,
      'test'
    );
    new DataFlowsIamRoles(
      this,
      'dataFlowLiveRoles',
      liveFS,
      this.caller,
      this.region,
      config.tags.environment,
      'live'
    );
  }

  // create new data-flows-prefect-filesystem S3 buckets
  // this is used for flow artifacts and staging as needed
  private createDataFlowsBucket(deploymentType: string): S3Bucket {
    const artifactsBucket = new S3Bucket(
      this,
      `dataFlowsPrefectFs${deploymentType}`,
      {
        bucket: `data-flows-prefect-fs-${config.tags.environment}-${deploymentType}`
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
    const prefix = `prefect-v2-agent-${config.tags.environment}-${deploymentType}`;
    const DeploymentTypeProper =
      deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1);

    const agentPolicies = new AgentIamPolicies(
      this,
      `agentPolicies${DeploymentTypeProper}`,
      this.dockerSecret,
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
          containerImage: config.agentImage,
          repositoryCredentialsParam: this.dockerSecret.arn,
          command: [
            'prefect',
            'agent',
            'start',
            '-q',
            `prefect-v2-queue-${config.tags.environment}-${deploymentType}`
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
