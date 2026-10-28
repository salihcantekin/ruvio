# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Reply converters and argument helpers shared by the command mixin."""

from __future__ import annotations

import json as _json
from datetime import timedelta
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Union

from ._models import AutoClaimResult, FillResult, GeoResult, IdemResult, LimitOutcome, PendingEntry, PendingSummary, StreamEntry

KeyT = Union[str, bytes]
Conv = Optional[Callable[[Any], Any]]

# --------------------------------------------------------------------------- converters


def _s(v: Any) -> Any:
    """Text for protocol words (state names, ids) regardless of decoding."""
    return v.decode() if type(v) is bytes else v


def _ok(r: Any) -> bool:
    # The hiredis parser returns b"OK" when replies are not decoded.
    return r == "OK" or r == b"OK"


def _bool(r: Any) -> bool:
    return r == 1


def _bools(r: Any) -> List[bool]:
    return [x == 1 for x in r]


def _float(r: Any) -> Optional[float]:
    return None if r is None else float(r)


def _floats(r: Any) -> List[Optional[float]]:
    return [None if x is None else float(x) for x in r]


def _int_or_none(r: Any) -> Optional[int]:
    return None if r is None else int(r)


def _pairs(r: Any) -> Dict[Any, Any]:
    if type(r) is dict:
        return r
    it = iter(r)
    return dict(zip(it, it))


def _pair_list(r: Any) -> List[Tuple[Any, Any]]:
    # Keeps order and repeated names, which a dict would drop.
    if r is None:
        return []
    if type(r) is dict:
        return list(r.items())
    if r and type(r[0]) is list:  # RESP3 nested pairs
        return [(k, v) for k, v in r]
    it = iter(r)
    return list(zip(it, it))


def _pairs_text_keys(r: Any) -> Dict[str, Any]:
    if type(r) is dict:
        return {_s(k): v for k, v in r.items()}
    it = iter(r)
    return {_s(k): v for k, v in zip(it, it)}


def _set(r: Any) -> set:  # type: ignore[type-arg]
    return set(r)


def _scored(r: Any) -> List[Tuple[Any, float]]:
    if r and type(r[0]) is list:  # RESP3 nested pairs
        return [(m, float(sc)) for m, sc in r]
    it = iter(r)
    return [(m, float(sc)) for m, sc in zip(it, it)]


def _scan(r: Any) -> Tuple[int, List[Any]]:
    return int(r[0]), r[1]


def _scan_pairs(r: Any) -> Tuple[int, Dict[Any, Any]]:
    return int(r[0]), _pairs(r[1])


def _scan_scored(r: Any) -> Tuple[int, List[Tuple[Any, float]]]:
    return int(r[0]), _scored(r[1])


def _key_value(r: Any) -> Optional[Tuple[Any, Any]]:
    return None if r is None else (r[0], r[1])


def _balances(r: Any) -> Optional[Tuple[int, int]]:
    return None if r is None else (int(r[0]), int(r[1]))


def _bzpop(r: Any) -> Optional[Tuple[Any, Any, float]]:
    return None if r is None else (r[0], r[1], float(r[2]))


def _mpop(r: Any) -> Optional[Tuple[Any, List[Any]]]:
    return None if r is None else (r[0], r[1])


def _fill(r: Any) -> FillResult:
    state = _s(r[0])
    if state == "STALE":
        return FillResult(state, r[1], r[2] if len(r) > 2 else None)
    if state == "HIT":
        return FillResult(state, r[1], None)
    if state == "FILL":
        return FillResult(state, None, r[1])
    return FillResult(state, None, None)


def _idem(r: Any) -> IdemResult:
    return IdemResult(_s(r[0]), r[1] if len(r) > 1 else None)


def _changes(r: Any) -> List[Tuple[int, Any]]:
    return [(int(row[0]), row[1]) for row in r]


def _set_reply(r: Any) -> bool:
    return r == "OK" or r == b"OK"


def _entries(r: Any) -> List[StreamEntry]:
    if r is None:
        return []
    return [StreamEntry(_s(e[0]), _pair_list(e[1])) for e in r]


def _xread(r: Any) -> Optional[List[Tuple[Any, List[StreamEntry]]]]:
    if r is None:
        return None
    if type(r) is dict:
        return [(k, _entries(v)) for k, v in r.items()]
    return [(row[0], _entries(row[1])) for row in r]


def _xpending(r: Any) -> PendingSummary:
    return PendingSummary(int(r[0]), _s(r[1]), _s(r[2]), [(_s(c[0]), int(c[1])) for c in (r[3] or [])])


def _xpending_range(r: Any) -> List[PendingEntry]:
    return [PendingEntry(_s(e[0]), _s(e[1]), int(e[2]), int(e[3])) for e in r]


def _xautoclaim(r: Any) -> AutoClaimResult:
    claimed = r[1]
    if claimed and type(claimed[0]) is list:
        claimed = _entries(claimed)
    else:
        claimed = [_s(x) for x in claimed]
    return AutoClaimResult(_s(r[0]), claimed, [_s(x) for x in (r[2] if len(r) > 2 else [])])


def _xclaim(r: Any) -> List[Any]:
    if r and type(r[0]) is list:
        return _entries(r)
    return [_s(x) for x in r]


def _xinfo_stream(r: Any) -> Dict[str, Any]:
    info = _pairs_text_keys(r)
    for k in ("first-entry", "last-entry"):
        e = info.get(k)
        if type(e) is list and len(e) == 2:
            info[k] = StreamEntry(_s(e[0]), _pair_list(e[1]))
    return info


def _list_of_dicts(r: Any) -> List[Dict[str, Any]]:
    return [_pairs_text_keys(x) for x in r]


def _geopos(r: Any) -> List[Optional[Tuple[float, float]]]:
    return [None if p is None else (float(p[0]), float(p[1])) for p in r]


def _geo_rows(withdist: bool, withhash: bool, withcoord: bool) -> Conv:
    if not (withdist or withhash or withcoord):
        return None

    def conv(r: Any) -> List[GeoResult]:
        out = []
        for row in r:
            i = 1
            dist = hsh = coord = None
            if withdist:
                dist = float(row[i])
                i += 1
            if withhash:
                hsh = int(row[i])
                i += 1
            if withcoord:
                coord = (float(row[i][0]), float(row[i][1]))
            out.append(GeoResult(row[0], dist, hsh, coord))
        return out

    return conv


def _info(r: Any) -> str:
    return r.decode() if type(r) is bytes else r


def _json_load(r: Any) -> Any:
    return None if r is None else _json.loads(r)


def _number(r: Any) -> Union[int, float, None]:
    if r is None:
        return None
    v = _json.loads(r)
    if type(v) is list and len(v) == 1:  # "$" paths may answer an array of one
        v = v[0]
    return v


def _text(r: Any) -> Any:
    return _s(r)


def _texts(r: Any) -> Any:
    return [_s(x) for x in r]


def _bf_info(r: Any) -> Any:
    if type(r) is list:
        return _pairs_text_keys(r)
    return r


def _hello(r: Any) -> Dict[str, Any]:
    return _pairs_text_keys(r)


def _limit(r: Any) -> Tuple[bool, int]:
    return (False, 0) if r is None else (True, int(r))


def _limit_outcome(r: Any) -> LimitOutcome:
    remaining, retry_after_ms = int(r[0]), int(r[1])
    return LimitOutcome(remaining >= 0, max(remaining, 0), max(retry_after_ms, 0))


def _bloom_seen(r: Any) -> List[bool]:
    # BF.MADD answers 1 for newly added items; "seen before" is the inverse
    return [x == 0 for x in r]


# --------------------------------------------------------------------------- helpers


def _flat_pairs(mapping: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]]) -> List[Any]:
    items = mapping.items() if isinstance(mapping, Mapping) else mapping
    out: List[Any] = []
    for k, v in items:
        out.append(k)
        out.append(v)
    return out


def _keys(keys: Union[KeyT, Iterable[KeyT]]) -> List[Any]:
    if isinstance(keys, (str, bytes)):
        return [keys]
    return list(keys)


def _limit_args(args: List[Any], offset: Optional[int], count: Optional[int]) -> None:
    if offset is not None or count is not None:
        if offset is None or count is None:
            raise ValueError("offset and count must be given together")
        args += ["LIMIT", offset, count]


def _ttl_args(ttl: Any) -> List[Any]:
    """``["EX", s]`` or ``["PX", ms]`` for a TTL-on-create argument (seconds or ``timedelta``).

    Whole seconds send ``EX``; a value with a sub-second part sends ``PX``. Zero, negative, ``bool`` and
    non-numeric values raise ``ValueError`` / ``TypeError`` before anything is sent.
    """
    if isinstance(ttl, timedelta):
        ms = ttl // timedelta(milliseconds=1)
        if ms <= 0:
            raise ValueError("ttl must be positive")
        return ["EX", ms // 1000] if ms % 1000 == 0 else ["PX", ms]
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
        raise TypeError("ttl must be seconds (int or float) or a timedelta")
    if not ttl > 0 or ttl == float("inf"):
        raise ValueError("ttl must be positive and finite")
    if isinstance(ttl, int) or ttl == int(ttl):
        return ["EX", int(ttl)]
    ms = int(round(ttl * 1000))
    if ms <= 0:
        raise ValueError("ttl must be at least 1 millisecond")
    return ["PX", ms]


def _expire_cond(nx: bool, xx: bool, gt: bool, lt: bool) -> List[str]:
    out = []
    if nx:
        out.append("NX")
    if xx:
        out.append("XX")
    if gt:
        out.append("GT")
    if lt:
        out.append("LT")
    return out
