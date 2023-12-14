from prefect import task, flow


@task()
def task_3():
    print("hello world")


@flow()
def flow_3():
    task_3()
