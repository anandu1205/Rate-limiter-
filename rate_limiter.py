import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, TypedDict, Union

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


@dataclass
class WindowState:
    timestamps: List[float] = field(default_factory=list)


@dataclass
class BucketState:
    tokens: float
    last_refill_ms: float


def _trunc_ms(value_ms: float) -> int:
    """Truncate a millisecond duration toward zero, never negative."""
    if value_ms <= 0:
        return 0
    return int(value_ms)  # int() on a positive float truncates toward zero


class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy = strategy
        self._lock = threading.Lock()

        # Policy A keys map to WindowState, Policy B keys map to BucketState.
        # Only one shape is ever populated for a given limiter instance,
        # since strategy is fixed at construction.
        self._state: Dict[Key, Union[WindowState, BucketState]] = {}

        self._total_allowed = 0
        self._total_rejected = 0

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(client_id, endpoint, limit, window_ms)
            else:
                result = self.policy_b_limit(client_id, endpoint, capacity, refill_rate)

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """
        Sliding-window log: keep timestamps of accepted requests, drop any
        strictly older than window_ms, and allow the request only if fewer
        than `limit` timestamps remain in the window.
        """
        key: Key = (client_id, endpoint)
        now = time.monotonic() * 1000.0
        cutoff = now - window_ms

        state = self._state.get(key)
        if state is None or not isinstance(state, WindowState):
            state = WindowState()
            self._state[key] = state

        state.timestamps = [t for t in state.timestamps if t > cutoff]

        if len(state.timestamps) < limit:
            state.timestamps.append(now)
            allowed = True
            remaining = limit - len(state.timestamps)
        else:
            allowed = False
            remaining = 0

        if state.timestamps:
            reset_after_ms = _trunc_ms(window_ms - (now - state.timestamps[0]))
        else:
            reset_after_ms = 0

        return allowed, remaining, reset_after_ms

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Token bucket: tokens refill continuously at `refill_rate` units/sec,
        capped at `capacity`. Each accepted request consumes 1 token.

        Contract: an accepted request always returns reset_after_ms == 0,
        regardless of how many whole tokens remain. reset_after_ms is only
        meaningful — and only computed — for rejected requests.
        """
        key: Key = (client_id, endpoint)
        now = time.monotonic() * 1000.0

        state = self._state.get(key)
        if state is None or not isinstance(state, BucketState):
            state = BucketState(tokens=float(capacity), last_refill_ms=now)
            self._state[key] = state

        elapsed_sec = max(0.0, (now - state.last_refill_ms) / 1000.0)
        state.tokens = min(capacity, state.tokens + elapsed_sec * refill_rate)
        state.last_refill_ms = now

        if state.tokens >= 1.0:
            state.tokens -= 1.0
            allowed = True
        else:
            allowed = False

        remaining = int(state.tokens)  # floor; tokens is always >= 0

        if allowed:
            # Contract: accepted requests never report a wait time, even if
            # the bucket is now below one token.
            reset_after_ms = 0
        elif refill_rate <= 0:
            # No refill ever happens; there's no meaningful ETA.
            reset_after_ms = 0
        else:
            deficit = 1.0 - state.tokens
            reset_after_ms = _trunc_ms((deficit / refill_rate) * 1000.0)

        return allowed, remaining, reset_after_ms

    def stats(self) -> StatsDict:
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }
