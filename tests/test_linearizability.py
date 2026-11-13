import math

from lsmkv.raft.linearizability import check_linearizable


def op(kind, invoke, ret, value=None, result=None, status="ok", key="x"):
    return {"op": kind, "key": key, "value": value, "invoke": invoke,
            "return": ret, "result": result, "status": status}


def test_sequential_history_is_linearizable():
    history = [op("put", 0, 1, value="a"), op("get", 2, 3, result="a"),
               op("delete", 4, 5), op("get", 6, 7, result=None)]
    assert check_linearizable(history) == (True, None)


def test_stale_read_is_rejected():
    history = [op("put", 0, 1, value="a"), op("put", 2, 3, value="b"),
               op("get", 4, 5, result="a")]
    assert check_linearizable(history) == (False, "x")


def test_read_of_a_value_never_written_is_rejected():
    assert check_linearizable([op("get", 0, 1, result="ghost")]) == (False, "x")


def test_concurrent_operations_may_take_either_order():
    writes = [op("put", 0, 10, value="a"), op("put", 0, 10, value="b")]
    assert check_linearizable(writes + [op("get", 11, 12, result="a")])[0]
    assert check_linearizable(writes + [op("get", 11, 12, result="b")])[0]


def test_real_time_order_is_enforced_across_overlap():
    # the read starts after put(b) returned, so it cannot see the older value
    history = [op("put", 0, 2, value="a"), op("put", 3, 5, value="b"),
               op("get", 6, 8, result="a")]
    assert not check_linearizable(history)[0]


def test_unknown_write_may_or_may_not_have_happened():
    lost = op("put", 0, math.inf, value="a", status="unknown")
    assert check_linearizable([lost, op("get", 5, 6, result="a")])[0]
    assert check_linearizable([lost, op("get", 5, 6, result=None)])[0]


def test_unknown_write_cannot_take_effect_before_it_was_sent():
    history = [op("get", 0, 1, result="a"),
               op("put", 5, math.inf, value="a", status="unknown")]
    assert not check_linearizable(history)[0]


def test_failed_operations_are_ignored_and_keys_are_independent():
    history = [op("put", 0, 1, value="a", status="failed"),
               op("get", 2, 3, result=None),
               op("put", 0, 1, value="z", key="y"), op("get", 2, 3, result="z", key="y")]
    assert check_linearizable(history) == (True, None)
