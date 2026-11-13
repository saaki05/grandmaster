"""Linearizability checker for key-value histories.

A history is linearizable if its operations can be placed in one total order
that (1) respects real time: an operation that returned before another was
invoked comes first, and (2) is a legal sequential execution of a register.
Linearizability is compositional (Herlihy and Wing, 1990), so each key is
checked on its own, which keeps the search small.

The search is the Wing and Gong backtracking algorithm with memoisation of
(remaining operations, register value) states, as in Lowe's refinement.

Operation records are dicts with: op ("put" | "get" | "delete"), key, value,
invoke, return, result, status. Status "ok" means the client got an answer.
"unknown" means it never heard back: the write may have taken effect at any
later time or never, so it is optional in the search. "failed" operations
definitely did not happen and are ignored.
"""
import math
from collections import defaultdict


def _check_key(ops):
    writes_and_reads = []
    for op in ops:
        if op["status"] == "failed":
            continue
        if op["status"] == "unknown" and op["op"] == "get":
            continue  # an unanswered read constrains nothing
        writes_and_reads.append(op)
    required = frozenset(i for i, op in enumerate(writes_and_reads) if op["status"] == "ok")
    everything = frozenset(range(len(writes_and_reads)))
    seen = set()

    def search(remaining, value):
        if not (remaining & required):
            return True
        state = (remaining, value)
        if state in seen:
            return False
        seen.add(state)
        # an operation may go next only if no remaining completed operation
        # returned before it was invoked
        horizon = min((writes_and_reads[i]["return"] for i in remaining), default=math.inf)
        for i in remaining:
            op = writes_and_reads[i]
            if op["invoke"] > horizon:
                continue
            if op["op"] == "get":
                if op["result"] != value:
                    continue
                next_value = value
            elif op["op"] == "put":
                next_value = op["value"]
            else:
                next_value = None
            if search(remaining - {i}, next_value):
                return True
        return False

    return search(everything, None)


def check_linearizable(history):
    """Return (True, None) if linearizable, else (False, key) for a key whose
    sub-history has no valid ordering."""
    by_key = defaultdict(list)
    for op in history:
        by_key[op["key"]].append(op)
    for key, ops in by_key.items():
        if not _check_key(ops):
            return False, key
    return True, None
