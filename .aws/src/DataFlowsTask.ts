import { Resource } from 'cdktf';
import { cloudwatch, datasources, ecs, s3 } from '@cdktf/provider-aws';
import { Construct } from 'constructs';
import { buildDefinitionJSON } from '@pocket-tools/terraform-modules';
import { config } from './config';
import { DataFlowsARN } from './DataFlowsARN';
import { DataFlowsEcsIam } from './DataFlowsEcsIam';

/**
 * This class creates ECS and IAM resources for executing Prefect Flows in ECS.
 */
export class DataFlowsTask extends Resource {
  /**
   * We create a custom ECS Task Definition to inject secrets as environment variables (currently for Snowflake and Dbt)
   * @private
   */
  public taskDefinition: ecs.EcsTaskDefinition;
  /**
   * Task and Execution role for the ECS task.
   * @private
   */
  public ecsIam: DataFlowsEcsIam;

  constructor(
    scope: Construct,
    name: string,
    private dependencies: {
      region: datasources.DataAwsRegion;
      caller: datasources.DataAwsCallerIdentity;
      imageUri: string;
      prefectStorageBucket: s3.S3Bucket;
    }
  ) {
    super(scope, name);

    // Create task and execution IAM roles
    this.ecsIam = new DataFlowsEcsIam(
      this,
      'data-flows-ecs-iam',
      this.dependencies.prefectStorageBucket
    );

    const flowContainerDefinitions = this.getFlowContainerDefinitions(
      this.createFlowLogGroup()
    );

    this.taskDefinition = this.createTaskDefinition(flowContainerDefinitions);
  }

  private createTaskDefinition(
    flowContainerDefinitions: string
  ): ecs.EcsTaskDefinition {
    const caller = this.dependencies.caller;

    return new ecs.EcsTaskDefinition(this, 'flow-task-definition', {
      family: `${config.prefix}-Flow`,
      containerDefinitions: flowContainerDefinitions,
      executionRoleArn: `arn:aws:iam::${caller.accountId}:role/${config.prefix}-TaskExecutionRole`,
      taskRoleArn: this.ecsIam.taskRole.arn,
      cpu: config.prefect.runTask.cpu.toString(),
      memory: config.prefect.runTask.memory.toString(),
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
    const dataFlowsARN = new DataFlowsARN(
      this.dependencies.region,
      this.dependencies.caller
    );

    const secretEnvVars = config.prefect.runTask.parameterStoreNames.map(
      (name) => ({
        name: name,
        valueFrom: dataFlowsARN.getParameterStoreArn(name),
      })
    );

    // Note, Prefect overrides attributes such as the command by passing kwargs into the boto3 runtask call. See:
    // https://github.com/PrefectHQ/prefect/blob/master/src/prefect/agent/ecs/agent.py
    const containerDefinition = buildDefinitionJSON({
      name: 'flow',
      containerImage: this.dependencies.imageUri,
      logGroup: flowLogGroup.name,
      logGroupRegion: this.dependencies.region.name,
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
