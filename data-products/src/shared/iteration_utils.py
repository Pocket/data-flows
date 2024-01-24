from typing import Generator, Sequence, TypeVar

T = TypeVar("T")


# For Generator type-hint explanation, see https://docs.python.org/3/library/typing.html#typing.Generator
def chunks(sequence: Sequence[T], n: int) -> Generator[Sequence[T], None, None]:
    """Yield successive n-sized chunks from the sequence."""
    for i in range(0, len(sequence), n):
        yield sequence[i : i + n]
