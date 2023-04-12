## Prefect v2 AWS Infrastructure

The goal of this project is to deploy the AWS infrastructure needed for running Prefect flows.

This setup is inspired by the Prefect recipes defined [here](https://docs.prefect.io/latest/recipes/recipes/).

The guide for how we leverage environments and different kinds of agents can be found [here](https://docs.prefect.io/latest/recipes/recipes/).

Prefect flow execution needs the following infrastructure components:

- ECS Clusters for Fargate Tasks.
- Custome docker image for the Prefect Agent.
- ECS Services for the Prefect Agent:

    - These services will need to have the proper IAM access to submit and monitor ECS tasks.
    - This will run on ECS Fargate.  We will deploy a `test` and `live` agent.  We call these `deployment_types`.  Flows pushed to the `dev-v2` branch will use the `test` agent and flows pushed to the `main-v2` branch will use the `live` agent.  This is how we provide development and production capabilities in a production grade Prefect environment.

- IAM roles and S3 resources for flows:
    
    - ECS tasks submitted for flows will need to have proper IAM access for executing the flow and for leveraging AWS resources in the flow.
    - We also need an S3 bucket for storing flow files and flow run artifacts.

- IAM roles for CircleCI OIDC based workflows

  - Flow deployments will leverage ECR for building and pushing docker images.  The flow CICD process will leverage Open ID Connect in CircleCI to interact with ECR safely.  This project creates those roles.

We have the ability to validate our infrastructure via the Development AWS Account.  Production deployments are applied to the Production AWS Account.

Finally, we are leveraging the [Terraform CDK (cdktf)](https://developer.hashicorp.com/terraform/tutorials/cdktf/cdktf-build) for defining infrastructure as code.  We are leveraging the [terraform-modules](https://github.com/Pocket/terraform-modules/) that Pocket manages.

The best way to navigate the AWS resource development is by starting with [src/main.ts](src/main.ts) and work backwards.

The custom docker image for the Prefect Agent lives at [docker/agent/Dockerfile](.docker/agent/Dockerfile).

All of the CICD required is orchestrated by CircleCI using the configuration defined at [../.circleci](../.circleci).