import re


def get_flow_name(file_path: str) -> str:
    return re.search(r'/flows/(?P<flow_name>.*)\.py', file_path).group('flow_name')
