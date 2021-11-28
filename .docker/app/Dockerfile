FROM prefecthq/prefect:0.15.9-python3.9 as base

ENV POCKET_APP_DIR=/pocket/app
COPY .docker/app $POCKET_APP_DIR

ENTRYPOINT ["tini", "-g", "--", "/pocket/app/entrypoint.sh"]
