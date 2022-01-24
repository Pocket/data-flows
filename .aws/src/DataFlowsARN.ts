import { datasources } from '@cdktf/provider-aws';
import { config } from './config';

/**
 * Helper class to format ARNs strings that we can't get by reference.
 */
export class DataFlowsARN {
  /**
   * Get the ARN for a DataFlow Parameter Store parameter.
   * @param region
   * @param caller
   * @param name Last part of the parameter name, e.g. 'API_KEY' for the parameter /DataFlows/Prod/API_KEY.
   * @private
   */
  public static getParameterArn(
    region: datasources.DataAwsRegion,
    caller: datasources.DataAwsCallerIdentity,
    name: string
  ): string {
    return `arn:aws:ssm:${region.name}:${caller.accountId}:parameter/${config.name}/${config.environment}/${name}`;
  }

  /**
   * Get the ARN of the execution role for ECS tasks that execute the Prefect Flows.
   * @param caller
   * @private
   */
  public static getFlowExecutionRoleArn(
    caller: datasources.DataAwsCallerIdentity
  ) {
    return `arn:aws:iam::${caller.accountId}:role/${config.prefix}-TaskExecutionRole`;
  }
}
