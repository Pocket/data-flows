FROM prefecthq/prefect:0.15.9-python3.9 as base

ENV POCKET_AGENT_DIR /pocket/agent
COPY . $POCKET_AGENT_DIR
# Choose an arbitrary path for the run task config file.
# Pocket's entrypoint.sh will create this file here based on the RUN_TASK_KWARGS environment variable.
ENV RUN_TASK_KWARGS_PATH /pocket/agent/run_task_kwargs.yml

ENTRYPOINT ["tini", "-g", "--", "/pocket/agent/entrypoint.sh"]
