# The prefecthq image contains "common" dependencies, such as AWS.
# It's defined by prefect as 'orchestration_extras' in setup.py here:
# @see https://github.com/PrefectHQ/prefect/blob/master/setup.py#L31-L45
FROM prefecthq/prefect:0.15.13-python3.9

# Install pipenv
RUN pip install pipenv

# Install python dependencies.
# - dependencies are managed by pipenv because it allows us to define direct dependencies, and lock in indirect ones.
# - dependencies are installed by pip, because Prefect installs dependencies globally.
WORKDIR /
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv lock -r > requirements.txt
RUN pipenv lock -r --dev-only > dev-requirements.txt
RUN echo "GIT_SHA=${GIT_SHA}"
RUN pip install -r requirements.txt
RUN if [ "$GIT_SHA" = "local" ]; then pip install -r dev-requirements.txt; fi

#Sentry GITSHA
ENV GIT_SHA=$GIT_SHA

# HACK: Stop Prefect ECS Agent from raising an exception when we provide a task_definition_arn and use Docker storage:
#       Cannot provide `task_definition_arn` when using `Docker` storage
#       This replaces the if-condition with "False". We should get this fixed in the Prefect repo and then upgrade.
RUN sed -i -E "s/if isinstance.*?, Docker\):/if False:/" /usr/local/lib/python3.9/site-packages/prefect/agent/ecs/agent.py

# Copy code
COPY src /src
ENV FLOWS_PATH=/src/flows/
