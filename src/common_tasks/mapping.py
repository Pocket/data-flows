from typing import Dict, List, Sequence, Tuple, TypeVar

from prefect import task

from utils.iteration import chunks


T = TypeVar('T')


@task()
def split_dict_of_lists_in_chunks(d: Dict[str, List[T]], chunk_size: int) -> List[Tuple[str, List[T]]]:
    result = []
    for k, v_list in d.items():
        result.extend((k, chunk) for chunk in chunks(v_list, n=chunk_size))
    return result


@task()
def split_in_chunks(sequence: Sequence[T], chunk_size: int) -> List[Sequence[T]]:
    """
    Splits the input into chunks.
    :param sequence: List or other type of sequence to split into chunks
    :param chunk_size: Maximum size of a chunk.
    :return: List of list, where the inner list is a chunk of user deltas of at most _chunk_size_ items.
    """
    return list(chunks(sequence, n=chunk_size))
