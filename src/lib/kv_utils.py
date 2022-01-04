from prefect.backend import set_key_value, get_key_value, delete_key, list_keys


def set_kv(key, value):
    return set_key_value(key, value)

def get_kv(key, default_value):
    try:
        return get_key_value(key)
    except ValueError as err:
        set_key_value(key, default_value)
        return get_key_value(key)
