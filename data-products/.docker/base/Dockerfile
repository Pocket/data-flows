ARG PYTHON_VERSION
FROM prefecthq/prefect:2-python${PYTHON_VERSION}

ENV PATH=/root/.local/bin:$PATH

RUN apt update && \
    apt install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt clean && apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

    
COPY dist ../common-utils
COPY src/shared shared
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml
RUN poetry config virtualenvs.create false
RUN poetry install --only main