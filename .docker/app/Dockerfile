# The prefecthq image contains "common" dependencies, such as AWS.
# It's defined by prefect as 'orchestration_extras' in setup.py here:
# @see https://github.com/PrefectHQ/prefect/blob/master/setup.py#L31-L45
FROM prefecthq/prefect:0.15.10-python3.9 as base

# Install pipenv
RUN pip install pipenv

# Install python dependencies in /.venv
WORKDIR /
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv lock -r > requirements.txt
RUN pipenv lock -r --dev-only > dev-requirements.txt
RUN echo "GIT_SHA=${GIT_SHA}"
RUN pip install -r requirements.txt
RUN if [ "$GIT_SHA" = "local" ]; then pip install -r dev-requirements.txt; fi

FROM base AS runtime

#Sentry GITSHA
ENV GIT_SHA=$GIT_SHA
