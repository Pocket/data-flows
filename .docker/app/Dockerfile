# The prefecthq image contains "common" dependencies, such as AWS.
# It's defined by prefect as 'orchestration_extras' in setup.py here:
# @see https://github.com/PrefectHQ/prefect/blob/master/setup.py#L31-L45
FROM prefecthq/prefect:1.1.0-python3.9
ARG GIT_SHA=local

# Install pipenv
RUN pip install 'pipenv==2023.7.23'

# Install python dependencies.
# - dependencies are managed by pipenv because it allows us to define direct dependencies, and lock in indirect ones.
# - dependencies are installed by pip, because Prefect installs dependencies globally.
WORKDIR /
COPY Pipfile .
COPY Pipfile.lock .
RUN echo "GIT_SHA=${GIT_SHA}"
RUN if [ "$GIT_SHA" = "local" ]; then pipenv install --system --dev; else pipenv install --system; fi

#Sentry GITSHA
ENV GIT_SHA=$GIT_SHA

# Google Cloud credentials are stored in a file at GOOGLE_APPLICATION_CREDENTIALS:
# https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable
ENV GOOGLE_APPLICATION_CREDENTIALS=/etc/gce-key.json

ENV DATA_FLOWS_SOURCE_DIR=/src
COPY src $DATA_FLOWS_SOURCE_DIR
# Adding the src directory to PYTHONPATH allows our modules to be imported in Python.
ENV PYTHONPATH="$DATA_FLOWS_SOURCE_DIR:${PYTHONPATH}"

# We use a custom entrypoint script. It's invoked using tini, just as in the official Docker image:
# https://github.com/PrefectHQ/prefect/blob/master/Dockerfile
COPY .docker/app/entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["tini", "-g", "--", "entrypoint.sh"]
