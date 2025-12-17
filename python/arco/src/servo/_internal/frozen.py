"""Small immutable collection utilities for SDK types."""
from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Generic, TypeVar

KT = TypeVar("KT")
VT = TypeVar("VT")


class FrozenDict(Mapping[KT, VT], Generic[KT, VT]):
    """An immutable, hashable mapping.

    This is intended for use inside frozen dataclasses so container fields are
    actually immutable in normal usage (unlike plain `dict` / `list`).
    """

    _data: dict[KT, VT]

    def __init__(
        self,
        mapping: Mapping[KT, VT] | Iterable[tuple[KT, VT]] = (),
        /,
        **kwargs: VT,
    ) -> None:
        self._data = dict(mapping, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        return self._data[key]

    def __iter__(self) -> Iterator[KT]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        return hash(frozenset(self._data.items()))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(self._data) == dict(other)
        return NotImplemented

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"

    def to_dict(self) -> dict[KT, VT]:
        """Convert to a plain dict (useful for JSON serialization)."""
        return dict(self._data)
