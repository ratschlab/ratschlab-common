from typing import Any, Sequence


def chunker(seq: Sequence[Any], size: int):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
