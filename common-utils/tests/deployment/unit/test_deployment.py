import os
from datetime import timedelta
from pathlib import Path, PosixPath
from unittest.mock import MagicMock, call, patch

import pytest
from boto3 import session
from moto import mock_ecs, mock_sts
from prefect import flow, task

from common.deployment import (
    GIT_SHA,
    CronSchedule,
    FlowDeployment,
    FlowDockerEnv,
    FlowEnvar,
    FlowSpec,
    IntervalSchedule,
    PrefectProject,
    ProjectEnvs,
    RRuleSchedule,
    get_aws_account_id,
    get_flow_folder,
    get_pyproject_metadata,
    run_command,
)

TEST_PYTHONPATH = os.path.expanduser(os.path.join(os.getcwd(), "tests/test_flows"))


@mock_sts
def test_get_aws_account_id():
    # Test sts call using moto sts mock.
    get_aws_account_id(MagicMock())


def test_run_command():
    # Simple test of run_command.
    # Validate that last line is returned as expected.
    x = run_command(
        """echo "this is a 
    multiline command that returns
    the last line"
    """
    )
    assert x.strip() == "the last line"


def test_run_command_execption():
    # Validate the run_command throws proper exception.
    with pytest.raises(Exception):
        run_command(
            """bad-command "this is a 
        multiline command that returns
        the last line"
        """
        )


def test_get_flow_folder():
    # Validate function returns slugified value.
    path = Path("tests/test_flows/flow_group_2/no_spec_example_flow.py")
    x = get_flow_folder(path)
    assert x == "flow-group-2"


@mock_sts
@patch("common.deployment.run_command")
def test_project_envs(mock_cmd):
    # Validate class methods using mock on run_command.
    cwd = os.path.expanduser(os.getcwd())
    pyproject_metadata = get_pyproject_metadata()
    x = ProjectEnvs(
        docker_envs=[
            FlowDockerEnv(
                project_name=pyproject_metadata.project_name,
                env_name="test-1",
                dockerfile_path=Path("tests/deployment/unit/testDockerfile1"),
                docker_build_context=Path("tests/deployment/unit"),
                python_version="3.10",
            ),
            FlowDockerEnv(
                project_name=pyproject_metadata.project_name,
                env_name="test-2",
                dockerfile_path=Path("tests/deployment/unit/testDockerfile2"),
                docker_build_context=Path("tests/deployment/unit"),
                python_version="3.10",
            ),
        ],
        pyproject_metadata=pyproject_metadata,
    )
    x.process()
    assert mock_cmd.call_count == 4
    assert mock_cmd.call_args_list == [
        call(
            f"{cwd}/src/common/deployment/build_image.sh common-utils-test-1-py-3.10 tests/deployment/unit/testDockerfile1 3.10 tests/deployment/unit"
        ),
        call(
            f"{cwd}/src/common/deployment/push_image.sh common-utils-test-1-py-3.10 123456789012"
        ),
        call(
            f"{cwd}/src/common/deployment/build_image.sh common-utils-test-2-py-3.10 tests/deployment/unit/testDockerfile2 3.10 tests/deployment/unit"
        ),
        call(
            f"{cwd}/src/common/deployment/push_image.sh common-utils-test-2-py-3.10 123456789012"
        ),
    ]


@mock_sts
@patch("common.deployment.run_command")
def test_project_envs_build_only(mock_cmd):
    cwd = os.path.expanduser(os.getcwd())
    # Validate class methods using mock on run_command.
    pyproject_metadata = get_pyproject_metadata()
    x = ProjectEnvs(
        docker_envs=[
            FlowDockerEnv(
                project_name=pyproject_metadata.project_name,
                env_name="test-1",
                dockerfile_path=Path("tests/deployment/unit/testDockerfile1"),
                docker_build_context=Path("tests/deployment/unit"),
                python_version="3.10",
            ),
            FlowDockerEnv(
                project_name=pyproject_metadata.project_name,
                env_name="test-2",
                dockerfile_path=Path("tests/deployment/unit/testDockerfile2"),
                docker_build_context=Path("tests/deployment/unit"),
                python_version="3.10",
            ),
        ],
        pyproject_metadata=pyproject_metadata,
    )
    x.process(True)
    assert mock_cmd.call_count == 2
    assert mock_cmd.call_args_list == [
        call(
            f"{cwd}/src/common/deployment/build_image.sh common-utils-test-1-py-3.10 tests/deployment/unit/testDockerfile1 3.10 tests/deployment/unit"
        ),
        call(
            f"{cwd}/src/common/deployment/build_image.sh common-utils-test-2-py-3.10 tests/deployment/unit/testDockerfile2 3.10 tests/deployment/unit"
        ),
    ]


@patch("common.deployment.DEPLOYMENT_TYPE", "main")
@patch("common.deployment.run_command")
def test_flow_deployment(mock_cmd):
    # Test schedule method return cli arg as expected.
    d1 = FlowDeployment(deployment_name="test", schedule=CronSchedule(cron="0 0 * * *"))  # type: ignore
    x1 = d1._get_schedule_arg()
    assert x1 == "--cron '0 0 * * *'"
    d2 = FlowDeployment(deployment_name="test", schedule=IntervalSchedule(interval=60))  # type: ignore
    x2 = d2._get_schedule_arg()
    assert x2 == "--interval 60"
    d3 = FlowDeployment(deployment_name="test", schedule=RRuleSchedule(rrule="FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17"))  # type: ignore
    x3 = d3._get_schedule_arg()
    assert (
        x3
        == "--rrule 'FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17'"
    )
    d4 = FlowDeployment(deployment_name="test")  # type: ignore
    x4 = d4._get_schedule_arg()
    assert x4 == ""

    # Validate push deployment method using mock on run_command.
    d5 = FlowDeployment(
        deployment_name="test",
        schedule=IntervalSchedule(interval=120),  # type: ignore
        cpu="1024",
        memory="4096",
        parameters={"test_param": "test_value"},
    )
    d5.push_deployment(
        project_name="test",
        infrastructure="test-ECS-block",
        flow_path=Path("tests/deployment/unit/test_deployment.py"),
        flow_function_name="test_function",
        flow_name="test.test.test",
        pythonpath_addition=TEST_PYTHONPATH,
        base_envars={"TEST": "test"},
    )
    assert mock_cmd.call_count == 1
    call_text = f"""export POCKET_PREFECT_FLOW_NAME=test.test.test && \\\n        export PYTHONPATH={TEST_PYTHONPATH} && \\\n        pushd ../ && \\\n        prefect deployment build common-utils/tests/deployment/unit/test_deployment.py:test_function \\\n        -n test-main \\\n        -sb github/data-flows-main/common-utils/tests/deployment/unit \\\n        -ib ecs-task/test-ECS-block \\\n        --override env.TEST=test --override \'task_customizations=[{{"op": "add", "path": "/overrides/cpu", "value": "1024"}}, {{"op": "add", "path": "/overrides/memory", "value": "4096"}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/subnets", "value": ["subnet-1234", "subnet-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/securityGroups", "value": ["sg-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/assignPublicIp", "value": "DISABLED"}}]\' \\\n        -q prefect-v2-queue-main \\\n        -v {GIT_SHA} \\\n        --params \'{{"test_param": "test_value"}}\' \\\n        -t test -t unit -t main \\\n        -a \\\n        --interval 120 --skip-upload && \\\n        popd"""
    mock_cmd.assert_called_with(call_text)


@patch("common.deployment.DEPLOYMENT_TYPE", "staging")
@patch("common.deployment.run_command")
def test_flow_deployment_prod_test(mock_cmd):
    # Test schedule method return cli arg as expected.
    d1 = FlowDeployment(deployment_name="test", schedule=CronSchedule(cron="0 0 * * *"))  # type: ignore
    x1 = d1._get_schedule_arg()
    assert x1 == ""
    d2 = FlowDeployment(deployment_name="test", schedule=IntervalSchedule(interval=60))  # type: ignore
    x2 = d2._get_schedule_arg()
    assert x2 == ""
    d3 = FlowDeployment(deployment_name="test", schedule=RRuleSchedule(rrule="FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17"))  # type: ignore
    x3 = d3._get_schedule_arg()
    assert x3 == ""
    d4 = FlowDeployment(deployment_name="test")  # type: ignore
    x4 = d4._get_schedule_arg()
    assert x4 == ""

    # Validate push deployment method using mock on run_command.
    d5 = FlowDeployment(
        deployment_name="test",
        schedule=IntervalSchedule(interval=120),  # type: ignore
        cpu="1024",
        memory="4096",
        parameters={"test_param": "test_value"},
    )
    d5.push_deployment(
        project_name="test",
        infrastructure="test-ECS-block",
        flow_path=Path("tests/deployment/unit/test_deployment.py"),
        flow_function_name="test_function",
        flow_name="test.test.test",
        pythonpath_addition=TEST_PYTHONPATH,
        base_envars={"TEST": "test"},
    )
    assert mock_cmd.call_count == 1
    call_text = f"""export POCKET_PREFECT_FLOW_NAME=test.test.test && \\\n        export PYTHONPATH={TEST_PYTHONPATH} && \\\n        pushd ../ && \\\n        prefect deployment build common-utils/tests/deployment/unit/test_deployment.py:test_function \\\n        -n test-staging \\\n        -sb github/data-flows-staging/common-utils/tests/deployment/unit \\\n        -ib ecs-task/test-ECS-block \\\n        --override env.TEST=test --override \'task_customizations=[{{"op": "add", "path": "/overrides/cpu", "value": "1024"}}, {{"op": "add", "path": "/overrides/memory", "value": "4096"}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/subnets", "value": ["subnet-1234", "subnet-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/securityGroups", "value": ["sg-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/assignPublicIp", "value": "DISABLED"}}]\' \\\n        -q prefect-v2-queue-staging \\\n        -v {GIT_SHA} \\\n        --params \'{{"test_param": "test_value"}}\' \\\n        -t test -t unit -t staging \\\n        -a \\\n         --skip-upload && \\\n        popd"""
    mock_cmd.assert_called_with(call_text)


@patch("common.deployment.DEPLOYMENT_TYPE", "dev")
@patch("common.deployment.run_command")
def test_flow_deployment_dev_test(mock_cmd):
    # Test schedule method return cli arg as expected.
    d1 = FlowDeployment(deployment_name="test", schedule=CronSchedule(cron="0 0 * * *"))  # type: ignore
    x1 = d1._get_schedule_arg()
    assert x1 == ""
    d2 = FlowDeployment(deployment_name="test", schedule=IntervalSchedule(interval=60))  # type: ignore
    x2 = d2._get_schedule_arg()
    assert x2 == ""
    d3 = FlowDeployment(deployment_name="test", schedule=RRuleSchedule(rrule="FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17"))  # type: ignore
    x3 = d3._get_schedule_arg()
    assert x3 == ""
    d4 = FlowDeployment(deployment_name="test")  # type: ignore
    x4 = d4._get_schedule_arg()
    assert x4 == ""

    # Validate push deployment method using mock on run_command.
    d5 = FlowDeployment(
        deployment_name="test",
        schedule=IntervalSchedule(interval=120),  # type: ignore
        cpu="1024",
        memory="4096",
        parameters={"test_param": "test_value"},
    )
    d5.push_deployment(
        project_name="test",
        infrastructure="test-ECS-block",
        flow_path=Path("tests/deployment/unit/test_deployment.py"),
        flow_function_name="test_function",
        flow_name="test.test.test",
        pythonpath_addition=TEST_PYTHONPATH,
        base_envars={"TEST": "test"},
    )
    assert mock_cmd.call_count == 1
    call_text = f"""export POCKET_PREFECT_FLOW_NAME=test.test.test && \\\n        export PYTHONPATH={TEST_PYTHONPATH} && \\\n        pushd ../ && \\\n        prefect deployment build common-utils/tests/deployment/unit/test_deployment.py:test_function \\\n        -n test-dev \\\n        -sb github/data-flows-dev/common-utils/tests/deployment/unit \\\n        -ib ecs-task/test-ECS-block \\\n        --override env.TEST=test --override \'task_customizations=[{{"op": "add", "path": "/overrides/cpu", "value": "1024"}}, {{"op": "add", "path": "/overrides/memory", "value": "4096"}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/subnets", "value": ["subnet-1234", "subnet-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/securityGroups", "value": ["sg-1234"]}}, {{"op": "add", "path": "/networkConfiguration/awsvpcConfiguration/assignPublicIp", "value": "DISABLED"}}]\' \\\n        -q prefect-v2-queue-dev \\\n        -v {GIT_SHA} \\\n        --params \'{{"test_param": "test_value"}}\' \\\n        -t test -t unit -t dev \\\n        -a \\\n         --skip-upload && \\\n        popd"""
    mock_cmd.assert_called_with(call_text)


@mock_ecs
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec(mock_ecs_save, mock_deployment):
    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        deployments=[FlowDeployment(deployment_name="base")],  # type: ignore
    )
    ecs_client = session.Session().client("ecs")
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs_client,
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 1
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )


@mock_ecs
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_all_fields(mock_ecs_save, mock_deployment):
    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret"
            )
        ],
        ephemeral_storage_gb=200,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
                envars=[FlowEnvar(envar_name="test", envar_value="test")],
            ),
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
                envars=[FlowEnvar(envar_name="test", envar_value="test")],
            ),
        ],  # type: ignore
    )
    ecs_client = session.Session().client("ecs")
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs_client,
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )


@mock_ecs
@patch("common.deployment.DEPLOYMENT_TYPE", "dev")
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_bad_env(mock_ecs_save, mock_deployment):
    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret"
            )
        ],
        envars=[FlowEnvar(envar_name="TEST", envar_value="test")],
        ephemeral_storage_gb=200,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
            ),  # type: ignore
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
            ),  # type: ignore
        ],  # type: ignore
    )
    ecs_client = session.Session().client("ecs")
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs_client,
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {"TEST": "test"},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )


BASE_TASK_DEF = {
    "family": "common-utils-flow-group-1-flow-1-dev",
    "taskRoleArn": "arn:aws:iam::123456789012:role/data-flows-prefect-dev-task-role",
    "executionRoleArn": "arn:aws:iam::123456789012:role/data-flows-prefect-dev-exec-role",
    "networkMode": "awsvpc",
    "containerDefinitions": [
        {
            "name": "prefect",
            "image": f"123456789012.dkr.ecr.us-east-1.amazonaws.com/data-flows-prefect-v2-envs:common-utils-base-py-3.10-{GIT_SHA}",
            "environment": [{"name": "DF_CONFIG_DEPLOYMENT_TYPE", "value": "dev"}],
            "secrets": [
                {
                    "name": "MY_SECRET_JSON",
                    "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:/my/secretsmanager/secret",
                }
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-create-group": "true",
                    "awslogs-group": "prefect",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "common-utils-flow-group-1-flow-1-dev",
                },
            },
            "mountPoints": [
                {
                    "sourceVolume": "prefect-scratch",
                    "containerPath": "/var/prefect-scratch",
                }
            ],
        }
    ],
    "requiresCompatibilities": [
        "FARGATE",
    ],
    "cpu": "256",
    "memory": "512",
    "ephemeralStorage": {"sizeInGiB": 200},
    "volumes": [
        {
            "name": "prefect-scratch",
        }
    ],
}


@mock_sts
@mock_ecs
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_handle_task_definition(mock_ecs_save, mock_deployment):
    test_session = session.Session()
    ecs = test_session.client("ecs")
    ecs.register_task_definition(**BASE_TASK_DEF)
    result = ecs.describe_task_definition(
        taskDefinition="common-utils-flow-group-1-flow-1-dev"
    )

    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret"
            )
        ],
        ephemeral_storage_gb=200,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
            ),  # type: ignore
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
            ),  # type: ignore
        ],  # type: ignore
    )
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs,
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )

    result = ecs.describe_task_definition(
        taskDefinition="common-utils-flow-group-1-flow-1-dev"
    )
    assert (
        result["taskDefinition"]["taskDefinitionArn"]
        == "arn:aws:ecs:us-east-1:123456789012:task-definition/common-utils-flow-group-1-flow-1-dev:1"
    )


@mock_sts
@mock_ecs
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_handle_task_definition_container_change(
    mock_ecs_save, mock_deployment
):
    test_session = session.Session()
    ecs = test_session.client("ecs")
    ecs.register_task_definition(**BASE_TASK_DEF)

    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON_CHANGED",
                envar_value="/my/secretsmanager/secret",
            )
        ],
        ephemeral_storage_gb=200,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
            ),  # type: ignore
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
            ),  # type: ignore
        ],  # type: ignore
    )
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs,
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )

    result = ecs.describe_task_definition(
        taskDefinition="common-utils-flow-group-1-flow-1-dev"
    )
    assert (
        result["taskDefinition"]["taskDefinitionArn"]
        == "arn:aws:ecs:us-east-1:123456789012:task-definition/common-utils-flow-group-1-flow-1-dev:2"
    )


@mock_sts
@mock_ecs
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_handle_task_definition_state_change(mock_ecs_save, mock_deployment):
    test_session = session.Session()
    ecs = test_session.client("ecs")
    ecs.register_task_definition(**BASE_TASK_DEF)

    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret"
            )
        ],
        ephemeral_storage_gb=50,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
            ),  # type: ignore
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
            ),  # type: ignore
        ],  # type: ignore
    )
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        ecs,
        pythonpath_addition=TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "common-utils-flow-group-1-flow-1-dev",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {},
    )
    mock_ecs_save.assert_called_with(
        "common-utils-flow-group-1-flow-1-dev", overwrite=True
    )

    result = ecs.describe_task_definition(
        taskDefinition="common-utils-flow-group-1-flow-1-dev"
    )
    assert (
        result["taskDefinition"]["taskDefinitionArn"]
        == "arn:aws:ecs:us-east-1:123456789012:task-definition/common-utils-flow-group-1-flow-1-dev:2"
    )


@mock_sts
@mock_ecs
@patch("common.deployment.FlowSpec.push_deployments")
def test_flow_project_flows_inner_exception(mock_deployment):
    # test flow with bad docker env will throw exception
    with pytest.raises(Exception) as e:
        x = PrefectProject()
        x.process_project_flows()
    assert "The following flows failed deployment: ['example_flow']" in str(e.value)
    assert mock_deployment.call_count == 1


@mock_sts
@mock_ecs
@patch("common.deployment.FlowSpec.push_deployments")
def test_flow_project_flows_multiple_inner_exception(mock_deployment):
    # force a second exeption
    mock_deployment.side_effect = Exception("misc")
    with pytest.raises(Exception) as e:
        x = PrefectProject()
        x.process_project_flows()
    assert (
        "The following flows failed deployment: ['example_flow', 'example_flow']"
        in str(e.value)
    )
    assert mock_deployment.call_count == 1


@mock_sts
@mock_ecs
@patch("common.deployment.FlowSpec.push_deployments")
def test_flow_project_flows_inner_exception_validate_only(mock_deployment):
    # test flow with bad docker env will throw exception
    with pytest.raises(Exception) as e:
        x = PrefectProject()
        x.process_project_flows(True)
    assert "The following flows failed validation: ['example_flow']" in str(e.value)
    assert mock_deployment.call_count == 0


@mock_sts
@patch("common.deployment.SourceFileLoader.load_module")
def test_flow_project_env_all_pass(mock_spec):
    @flow()
    def flow_1():
        print("hello world!")

    mock_spec.return_value = FlowSpec(flow=flow_1, docker_env="base")  # type: ignore
    x = PrefectProject()
    x.process_project_flows(True)
    assert mock_spec.call_count == 3


@mock_sts
@patch("common.deployment.run_command")
def test_flow_project_envs(mock_cmd):
    x = PrefectProject()
    x.process_project_docker_envs()
    assert mock_cmd.call_count == 2
