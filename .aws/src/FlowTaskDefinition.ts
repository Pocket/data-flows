import { Resource } from 'cdktf';
import { cloudwatch, datasources, ecs, iam } from '@cdktf/provider-aws';
import { Construct } from 'constructs';
import { buildDefinitionJSON } from '@pocket-tools/terraform-modules';
import { config } from './config';
import { DataFlowsARN } from './DataFlowsARN';

/**
 * ECS Task Definition for executing Prefect Flows in ECS.
 * We create a custom ECS Task Definition to inject secrets as environment variables (currently for Snowflake and Dbt)
 * Note that the Prefect ECS agent sets additional kwargs when it calls ecs_client.run_task, e.g. the Docker command
 * that determines which Prefect Flow is executed.
 * @see https://github.com/PrefectHQ/prefect/blob/master/src/prefect/agent/ecs/agent.py
 */
export class FlowTaskDefinition extends Resource {
  public taskDefinition: ecs.EcsTaskDefinition;

  constructor(
    scope: Construct,
    name: string,
    private dependencies: {
      region: datasources.DataAwsRegion;
      caller: datasources.DataAwsCallerIdentity;
      imageUri: string;
      taskRole: iam.IamRole;
    }
  ) {
    super(scope, name);

    const logGroup = this.createFlowLogGroup();
    const flowContainerDefinitions = this.getFlowContainerDefinitions(logGroup);
    this.taskDefinition = this.createTaskDefinition(flowContainerDefinitions);
  }

  private createTaskDefinition(
    flowContainerDefinitions: string
  ): ecs.EcsTaskDefinition {
    const caller = this.dependencies.caller;

    return new ecs.EcsTaskDefinition(this, 'flow-task-definition', {
      family: `${config.prefix}-Flow`,
      containerDefinitions: flowContainerDefinitions,
      executionRoleArn: DataFlowsARN.getFlowExecutionRoleArn(caller),
      taskRoleArn: this.dependencies.taskRole.arn,
      cpu: config.prefect.flowTask.cpu.toString(),
      memory: config.prefect.flowTask.memory.toString(),
      requiresCompatibilities: ['FARGATE'],
      networkMode: 'awsvpc',
      tags: config.tags,
    });
  }

  /**
   * Get the ECS container definition for the container running the Prefect Flow.
   * @see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.register_task_definition
   * @return ECS container definition object (should be formatted as JSON when assigning it to the task definition)
   */
  private getFlowContainerDefinitions(
    flowLogGroup: cloudwatch.CloudwatchLogGroup
  ): string {
    const region = this.dependencies.region;
    const caller = this.dependencies.caller;

    const secretEnvVars = config.prefect.flowTask.parameterStoreNames.map(
      (name) => ({
        name: name,
        valueFrom: DataFlowsARN.getParameterArn(region, caller, name),
      })
    );

    // Note, Prefect overrides attributes such as the command by passing kwargs into the boto3 runtask call. See:
    // https://github.com/PrefectHQ/prefect/blob/master/src/prefect/agent/ecs/agent.py
    const containerDefinition = buildDefinitionJSON({
      name: 'flow',
      containerImage: this.dependencies.imageUri,
      logGroup: flowLogGroup.name,
      logGroupRegion: region.name,
      envVars: [
        // Prefect's ECS agent sets this environment variable, but I couldn't find what purpose it serves.
        {
          name: 'PREFECT__CONTEXT__IMAGE',
          value: this.dependencies.imageUri,
        },
      ],
      secretEnvVars: secretEnvVars,
      essential: true,
    });

    // Terraform-Modules buildDefinitionJSON returns a single container definition, but ECS needs an array.
    return `[${containerDefinition}]`;
  }

  /**
   * Create a log group for the flow container.
   * @private
   */
  private createFlowLogGroup(): cloudwatch.CloudwatchLogGroup {
    return new cloudwatch.CloudwatchLogGroup(this, 'ecs-flow-log-group', {
      namePrefix: `/ecs/${config.prefix}/flow`,
      retentionInDays: 30,
      tags: config.tags,
    });
  }
}
