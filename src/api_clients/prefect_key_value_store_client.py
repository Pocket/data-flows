from prefect.backend import set_key_value, get_key_value, delete_key, list_keys


def set_kv(key: str, value: str):
    return set_key_value(key, value)

def get_kv(key: str, default_value: str):
    try:
        return get_key_value(key)
    except ValueError as err:
        set_key_value(key, default_value)
        return get_key_value(key)
