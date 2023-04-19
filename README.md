# data-flows
Data flows orchestrated using [Prefect](https://prefect.io) Version 2.


This project is comprised of the the following subprojects:

- [.aws](.aws)

    This contains the Terraform CDK code for the AWS resources needed for this project.  It also contains a custom docker image for the Prefect v2 agent.

- [.circleci](.circleci)

    This contains CircleCI configuration for managing the CICD process for this project.

- [common-utils](common-utils)

    This contains common Python utilities to use for Prefect v2 flow development and deployment.

- [data-products](data-products)

    This contains the Python code for Prefect v2 flows owned by the Data Products Team.

This is built as a **monorepo**, which means new subprojects for Python code can be added as needed including for teams that want to build their own flows on Prefect v2.