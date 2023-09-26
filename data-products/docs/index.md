# Getting Started

`data_products` consists of [Prefect](https://docs.prefect.io) flows and shared utilities.  All flows in the project can leverage these shared utilities for the use of common abstractions specific to this project. Flows themselves live in a folder specific to that flow, which can also contain specific helpers to aid in flow development.

Flows can be thought of as data applications that run on a specific schedule or on-demand via manual execution.

Flows and flow specific code lives in separate folders in `src`.  For example, `src/sql_etl`.  Everything in that folder will be available to the flow at run time.

Shared utilities live in `src/shared` can will be available to all flows.

## Installation and Setup

This project uses [Poetry](https://python-poetry.org/) for dependency management.
We also use the (Poetry dotenv plugin)[https://github.com/volopivoshenko/poetry-plugin-dotenv] to apply environment variables at runtime through the use of a `.env` file that should live at the root of this project.

!!! note 
    If you get a dependency error on installion of the plug, you just need to update Poetry using `poetry self update`.

These should be deployed using the details offered by each project's respective documentation.

Once all Poetry requirements are installed.  You can install all project dependencies through the `poetry install` command.  This will install both `dev` and `main` sets of dependencies.
