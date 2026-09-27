# RateLimiter

A thread-safe, in-memory rate limiter supporting two interchangeable strategies:

- **Policy A — Sliding Window Log**: allow at most `limit` requests per `(client_id, endpoint)` within the most recent `window_ms` milliseconds.
- **Policy B — Token Bucket**: maintain a per-`(client_id, endpoint)` budget that refills continuously at `refill_rate` units/second, capped at `capacity`; each accepted request consumes 1 unit.

## Usage

```python
from rate_limiter import RateLimiter

limiter = RateLimiter(strategy="policy_a")  # or "policy_b"

allowed, remaining, reset_after_ms = limiter.check(
    client_id="user-123",
    endpoint="/api/search",
    limit=10,          # used by policy_a
    window_ms=60_000,  # used by policy_a
    capacity=10,        # used by policy_b
    refill_rate=0.5,    # used by policy_b, units/sec
)

if not allowed:
    print(f"Rate limited. Retry in {reset_after_ms}ms")
```

The strategy is fixed at construction time; `check()` always passes both sets of parameters, but only the ones relevant to the active strategy are used.

### Return contract

`check()` returns `(allowed, remaining, reset_after_ms)`:

| Field | Meaning |
|---|---|
| `allowed` | Whether this request was permitted. |
| `remaining` | Capacity left immediately after this decision. |
| `reset_after_ms` | Milliseconds until capacity becomes available again. **For Policy B, this is always `0` on an accepted request** — the wait time is only meaningful (and only computed) when a request is rejected. |

### Stats

```python
limiter.stats()
# {
#   "strategy": "policy_a",
#   "total_allowed": 42,
#   "total_rejected": 3,
#   "active_keys": 7,
# }
```

`active_keys` counts distinct `(client_id, endpoint)` pairs currently tracked under the active policy.

## Design notes

**Locking.** A single `threading.Lock` covers the full read → decide → mutate → aggregate-counter sequence inside `check()`. This is the correct boundary for a scoped, single-process server: the decision must be atomic with the state mutation, or two concurrent requests could both read the same pre-decrement state and both be allowed past a limit that should only admit one of them.

**Monotonic time.** Both policies use `time.monotonic()` rather than `time.time()`. Wall-clock time can jump backward (NTP adjustments, manual clock changes), which would let a client's window or bucket appear to un-expire or over-refill. Monotonic time only moves forward, so elapsed-time math (`now - last_seen`) is always non-negative and safe to use directly in refill and window calculations.

**Per-key state shape.** Policy A stores a list of accepted-request timestamps per key (`WindowState`); Policy B stores a `(tokens, last_refill_time)` pair per key (`BucketState`). The two are kept as distinct typed structures rather than a single loosely-typed store, since a limiter instance only ever uses one shape for the lifetime of its fixed `strategy`.

**Timer truncation.** `reset_after_ms` values are truncated toward zero (never rounded up), so a caller that retries exactly at the reported time never finds itself still rate-limited. Policy B additionally guarantees `reset_after_ms == 0` whenever a request is *accepted*, even if the bucket is left with a fractional token balance below 1 — the wait time is only informative to a caller that was turned away.

## Scaling beyond a single process

The current implementation keeps all state in an in-process `dict` guarded by a single lock, which works for one server instance but not for a fleet behind a load balancer, since each instance would enforce its own independent limits.

To move to a shared store (e.g., Redis):

- Policy A's sliding window maps naturally onto a sorted set per key (`ZADD`/`ZREMRANGEBYSCORE`/`ZCARD`), with the prune-then-count-then-add sequence wrapped in a Lua script or `MULTI`/`WATCH` transaction to preserve atomicity without a process-local lock.
- Policy B's token bucket maps onto a small Lua script that reads `(tokens, last_refill)`, computes the refill, decides, and writes back in one atomic round trip — Redis's single-threaded script execution gives the same atomicity guarantee the in-process lock provides today.
- The `threading.Lock` would be removed entirely in that design; the shared store's atomic operations become the concurrency boundary instead.

## Testing

Recommended property to assert directly against the contract above:

> For Policy B, every accepted decision has `reset_after_ms == 0`, regardless of the resulting token balance.
