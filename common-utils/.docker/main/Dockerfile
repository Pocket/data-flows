ARG PYTHON_VERSION
FROM prefecthq/prefect:2-python${PYTHON_VERSION}

ENV PATH=/root/.local/bin:$PATH

RUN apt update && \
    apt install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt clean && apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY . /
RUN poetry install --only dev