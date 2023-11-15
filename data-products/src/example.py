from prefect import flow


@flow(name='test')
def main():
    print('hello world')