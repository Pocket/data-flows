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
import { DataAwsS3Bucket } from '@cdktf/provider-aws/lib/data-aws-s3-bucket';

// main Terraform Stack object for Prefect V2 infrastructure
class PrefectV2 extends TerraformStack {
  // these will enable access to variables in private methods
  private readonly region: DataAwsRegion;
  private readonly caller: DataAwsCallerIdentity;
  private readonly prefectV2Secret: DataAwsSecretsmanagerSecret;
  private readonly dockerSharedSecret: DataAwsSecretsmanagerSecret;

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
    // need this for docker hub pull
    this.dockerSharedSecret = new DataAwsSecretsmanagerSecret(
      this,
      'dockerSharedSecret',
      {
        name: 'Shared/DockerHub'
      }
    );
    // need this to support article text flow
    const pocketDataItemBucket = new DataAwsS3Bucket(
      this,
      `pocketDataItemBucket`,
      {
        bucket: `${config.pocketDataItemsBucket}`
      }
    );

    // we need an agent per queue NOTE: this will be deprecated in favor of workers
    // worker per environment
    // AWS dev will have the dev agent
    // AWS production will have the staging and main agent
    // we also need an s3 bucket per queue and set of iam roles
    // these map to github branch strategy of dev-v2, staging-v2, and main-v2
    if (config.isDev) {
      this.getAgentService('dev');
      this.getWorkerService('dev');
      const devS3Bucket = this.createDataFlowsBucket('dev');
      new DataFlowsIamRoles(
        this,
        'dataFlowDevRoles',
        devS3Bucket,
        pocketDataItemBucket,
        this.caller,
        this.region,
        'dev'
      );
    } else {
      this.getAgentService('staging');
      this.getWorkerService('staging');
      const stagingS3Bucket = this.createDataFlowsBucket('staging');
      new DataFlowsIamRoles(
        this,
        'dataFlowStagingRoles',
        stagingS3Bucket,
        pocketDataItemBucket,
        this.caller,
        this.region,
        'staging'
      );
      this.getAgentService('main');
      this.getWorkerService('main');
      const mainS3Bucket = this.createDataFlowsBucket('main');
      new DataFlowsIamRoles(
        this,
        'dataFlowMainRoles',
        mainS3Bucket,
        pocketDataItemBucket,
        this.caller,
        this.region,
        'main'
      );
    }
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
      this.dockerSharedSecret,
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
          containerImage: `${this.caller.accountId}.dkr.ecr.${this.region.name}.amazonaws.com/data-flows-prefect-v2-envs:prefect-agent-${config.imageTag}`,
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
  // create a task definition and service using private methods and params
  private getWorkerService(deploymentType: string) {
    const prefix = `prefect-v2-worker-${deploymentType}`;
    const DeploymentTypeProper =
      deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1);

    const agentPolicies = new AgentIamPolicies(
      this,
      `workerPolicies${DeploymentTypeProper}`,
      this.prefectV2Secret,
      this.dockerSharedSecret,
      prefix,
      this.caller,
      this.region
    );

    // create the ECS Service for the Prefect v2 agent
    new PocketECSApplication(this, prefix, {
      prefix: prefix,
      shortName: `PFCTWKR${deploymentType.toUpperCase()}`,
      taskSize: {
        cpu: config.agentCpu,
        memory: config.agentMemory
      },
      autoscalingConfig: {
        targetMaxCapacity: 2
      },
      containerConfigs: [
        {
          name: `prefect-v2-worker`,
          containerImage: 'prefecthq/prefect:2-latest',
          repositoryCredentialsParam: this.dockerSharedSecret.arn,
          command: [
            '/bin/sh',
            '-c',
            `pip install prefect-aws && prefect worker start --pool mozilla-aws-ecs-fargate-${deploymentType} --type ecs`
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

class PrefectOidc extends TerraformStack {
  constructor(scope: Construct, id: string) {
    super(scope, id);

    new AwsProvider(this, 'AWS', {
      region: config.region,
      defaultTags: {
        tags: config.tags
      }
    });

    // boiler plate for access to region and account id from iam creds
    const region = new DataAwsRegion(this, 'region');
    const caller = new DataAwsCallerIdentity(this, 'caller');

    // create new data-flows-prefect-v2-envs ECR Repo
    new ApplicationECR(this, 'data-flows-prefect-v2-envs-ecr', {
      name: 'data-flows-prefect-v2-envs'
    });

    // create the CircleCI OpenId Role for Image Upload
    new CircleCiOIDC(this, 'CircleCiOIDC', region, caller);
  }
}

// setup our App and add our stack(s)
const app = new App();
const prefectStack = new PrefectV2(app, 'prefect-v2');
const prefectOidc = new PrefectOidc(app, 'prefect-oidc');

new CloudBackend(prefectStack, {
  hostname: 'app.terraform.io',
  organization: 'Pocket',
  workspaces: new NamedCloudWorkspace(`prefect-v2-${config.tags.environment}`)
});
new CloudBackend(prefectOidc, {
  hostname: 'app.terraform.io',
  organization: 'Pocket',
  workspaces: new NamedCloudWorkspace(
    `prefect-v2-circleci-${config.tags.environment}`
  )
});

app.synth();
