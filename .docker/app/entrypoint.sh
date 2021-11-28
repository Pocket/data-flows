# Prefect requires the run task kwargs to be stored in a YAML file, so we have a Python script to create it
export RUN_TASK_KWARGS_PATH=$POCKET_APP_DIR/run_task_kwargs.yml
python "$POCKET_APP_DIR/create_run_task_kwargs.py" -o "$RUN_TASK_KWARGS_PATH" --verbose

# Start Prefect's entrypoint
# - Prefect's entrypoint is located at /usr/local/bin/entrypoint.sh
# - $* is a string with all Docker commands, something like `prefect agent ecs start --cluster DataFlows-Prod`
# - We set --run-task-kwargs to the above path
exec entrypoint.sh $@ --run-task-kwargs $RUN_TASK_KWARGS_PATH
