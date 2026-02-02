import itertools
from collections.abc import Iterable


def batched(iterable: Iterable, n: int):
    """
    Get batched result from an iterator
    Ref: https://docs.python.org/3/library/itertools.html#itertools.batched

    NOTE: somehow even using Python 3.12.7 still returning error with itertools.batched.
    """
    if n < 1:
        raise ValueError("n must be at least one")

    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch
