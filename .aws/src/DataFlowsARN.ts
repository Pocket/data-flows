import { datasources } from '@cdktf/provider-aws';
import { config } from './config';

/**
 * Helper class to format ARNs
 */
export class DataFlowsARN {
  /**
   * region:caller, for example us-east-1:45321
   * @private
   */
  private readonly region_caller: string;

  constructor(
    private region: datasources.DataAwsRegion,
    private caller: datasources.DataAwsCallerIdentity
  ) {
    this.region_caller = `${this.region.name}:${this.caller.accountId}`;
  }

  /**
   * Get the ARN for a DataFlow Parameter Store parameter.
   * @param name Last part of the parameter name, e.g. 'API_KEY' for the parameter /DataFlows/Prod/API_KEY.
   * @private
   */
  public getParameterStoreArn(name: string): string {
    return `arn:aws:ssm:${this.region_caller}:parameter/${config.name}/${config.environment}/${name}`;
  }
}
