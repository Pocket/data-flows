// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0
import { Construct } from "constructs";
import { AwsProvider } from "@cdktf/provider-aws/lib/provider";
import { config } from "./config";
import { App, TerraformStack, CloudBackend, NamedCloudWorkspace } from "cdktf";
import { DataAwsRegion } from "@cdktf/provider-aws/lib/data-aws-region";
import { DataAwsCallerIdentity } from "@cdktf/provider-aws/lib/data-aws-caller-identity";
import { CloudwatchLogGroup } from "@cdktf/provider-aws/lib/cloudwatch-log-group";
import { EcsCluster } from "@cdktf/provider-aws/lib/ecs-cluster";
import { EcsClusterCapacityProviders } from "@cdktf/provider-aws/lib/ecs-cluster-capacity-providers";
import { AgentIamRoles } from "./iam";
import { DataAwsSecretsmanagerSecret } from "@cdktf/provider-aws/lib/data-aws-secretsmanager-secret";

class PrefectV2 extends TerraformStack {
  constructor(scope: Construct, id: string) {
    super(scope, id);

    new AwsProvider(this, "AWS", {
      region: config.region,
      defaultTags: {
        tags: config.tags
      }
    });

    new DataAwsRegion(this, "region");
    const caller = new DataAwsCallerIdentity(this, "caller");

    const dockerSecret = new DataAwsSecretsmanagerSecret(this, 'dockerSecret', {
      name: "Shared/DockerHub"
    })

    const prefectV2Secret = new DataAwsSecretsmanagerSecret(this, "prefectV2Secret", {
      name: `dpt/${config.tags.environment}/prefect_v2`
    })

    new CloudwatchLogGroup(this, "logGroup", {
      name: `prefect-v2-agent-log-group-${config.tags.environment}`,
      retentionInDays: config.log_retention_days
    });

    const ecsCluster = new EcsCluster(this, 'ecsCluster', {
      name: `prefect-v2-${config.tags.environment}` 
    });

    new EcsClusterCapacityProviders(this, "ecsCapacityProvider", {
      clusterName: ecsCluster.name,
      capacityProviders: ["FARGATE"]
    })

    new AgentIamRoles(this, 'agentRoles', dockerSecret, prefectV2Secret, ecsCluster, caller )

    // Agent Service 
    // Data Flows Repo Services
  }
}

const app = new App();
const stack = new PrefectV2(app, "prefect-v2");
new CloudBackend(stack, {
  hostname: "app.terraform.io",
  organization: "Pocket",
  workspaces: new NamedCloudWorkspace(config.workspaceName)
});
app.synth();
