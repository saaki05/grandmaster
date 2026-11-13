import random

import pytest

from lsmkv.raft.linearizability import check_linearizable
from lsmkv.raft.node import LEADER, RaftNode
from lsmkv.raft.sim import Cluster, SafetyViolation
from lsmkv.raft.statemachine import LSMStateMachine
from lsmkv.raft.storage import FileStorage


def has_leader(cluster):
    return lambda: cluster.leader() is not None


def committed_everywhere(cluster, record):
    return lambda: record["status"] == "ok" and all(
        n.last_applied >= max(cluster.applied) for n in cluster.nodes.values())


@pytest.mark.parametrize("size", [1, 3, 5])
def test_a_healthy_cluster_elects_exactly_one_leader(size):
    cluster = Cluster(size=size, seed=1)
    assert cluster.run_until(has_leader(cluster))
    cluster.run(100)
    assert sum(n.role == LEADER for n in cluster.nodes.values()) == 1
    assert len({n.term for n in cluster.nodes.values()}) == 1


def test_a_command_is_replicated_and_applied_on_every_node():
    cluster = Cluster(size=3, seed=2)
    cluster.run_until(has_leader(cluster))
    record = cluster.submit(cluster.leader().id, "put", "k", "v")
    assert cluster.run_until(committed_everywhere(cluster, record))
    assert [m.data for m in cluster.machines.values()] == [{"k": "v"}] * 3


def test_followers_reject_client_commands():
    cluster = Cluster(size=3, seed=3)
    cluster.run_until(has_leader(cluster))
    follower = next(i for i, n in cluster.nodes.items() if n.role != LEADER)
    assert cluster.submit(follower, "put", "k", "v") is None


def test_committed_data_survives_leader_crash_and_old_leader_catches_up():
    cluster = Cluster(size=3, seed=4)
    cluster.run_until(has_leader(cluster))
    old = cluster.leader().id
    first = cluster.submit(old, "put", "a", "1")
    cluster.run_until(lambda: first["status"] == "ok")
    cluster.crash(old)

    assert cluster.run_until(has_leader(cluster))
    assert cluster.leader().id != old
    second = cluster.submit(cluster.leader().id, "put", "b", "2")
    assert cluster.run_until(lambda: second["status"] == "ok")

    cluster.restart(old)
    assert cluster.run_until(lambda: cluster.machines[old].data == {"a": "1", "b": "2"})


def test_isolated_leader_cannot_commit_and_its_entry_is_discarded():
    cluster = Cluster(size=5, seed=5)
    cluster.run_until(has_leader(cluster))
    old = cluster.leader().id
    others = [i for i in cluster.ids if i != old]
    cluster.partition([old, others[0]], others[1:])

    stranded = cluster.submit(old, "put", "k", "minority")
    cluster.run(200)
    assert stranded["status"] == "unknown"  # no quorum, never acknowledged

    majority_leader = max((cluster.nodes[i] for i in others[1:] if cluster.nodes[i].role == LEADER),
                          key=lambda n: n.term)
    winner = cluster.submit(majority_leader.id, "put", "k", "majority")
    assert cluster.run_until(lambda: winner["status"] == "ok")

    cluster.heal()
    assert cluster.run_until(
        lambda: all(m.data == {"k": "majority"} for m in cluster.machines.values()))
    assert stranded["status"] == "failed"
    cluster.check_log_matching()


def test_no_progress_without_a_quorum():
    cluster = Cluster(size=3, seed=6)
    cluster.run_until(has_leader(cluster))
    leader = cluster.leader().id
    for node_id in cluster.ids:
        if node_id != leader:
            cluster.crash(node_id)
    record = cluster.submit(leader, "put", "k", "v")
    cluster.run(300)
    assert record["status"] == "unknown"
    assert cluster.machines[leader].data == {}


def test_progress_over_a_lossy_duplicating_network():
    cluster = Cluster(size=3, seed=7, drop_rate=0.2, duplicate_rate=0.2, delay=(1, 8))
    assert cluster.run_until(has_leader(cluster), max_ticks=5000)
    done = []
    for i in range(20):
        cluster.run_until(has_leader(cluster), max_ticks=5000)
        done.append(cluster.submit(cluster.leader().id, "put", f"k{i}", str(i)))
        cluster.run(30)
    cluster.drop_rate = cluster.duplicate_rate = 0.0
    cluster.run(500)
    acknowledged = {r["key"]: r["value"] for r in done if r and r["status"] == "ok"}
    assert len(acknowledged) >= 10
    for machine in cluster.machines.values():
        assert acknowledged.items() <= machine.data.items()
    assert len({tuple(sorted(m.data.items())) for m in cluster.machines.values()}) == 1


def test_file_storage_round_trip_truncation_and_torn_tail(tmp_path):
    storage = FileStorage(str(tmp_path))
    storage.save_state(3, "n1")
    for i in range(5):
        storage.append({"term": 1, "cmd": {"id": i}})
    storage.truncate_from(4)
    storage.append({"term": 3, "cmd": {"id": 99}})
    storage.close()
    with open(tmp_path / "raft_log.jsonl", "ab") as f:
        f.write(b'deadbeef {"term": 3, "cm')  # crash mid-append

    reopened = FileStorage(str(tmp_path))
    assert (reopened.term, reopened.voted_for) == (3, "n1")
    assert reopened.last_index() == 4
    assert [reopened.entry(i)["cmd"]["id"] for i in range(1, 5)] == [0, 1, 2, 99]
    assert reopened.term_at(0) == 0


def test_cluster_on_durable_storage_and_lsm_state_machines(tmp_path):
    cluster = Cluster(
        size=3, seed=8,
        storage_factory=iter(FileStorage(str(tmp_path / f"raft{i}")) for i in range(3)).__next__,
        state_machine_factory=lambda node_id: LSMStateMachine(str(tmp_path / node_id),
                                                              memtable_limit=4))
    cluster.run_until(has_leader(cluster))
    records = []
    for i in range(12):
        cluster.run_until(has_leader(cluster))
        records.append(cluster.submit(cluster.leader().id, "put", f"k{i:02d}", str(i)))
        cluster.run(10)
        if i == 5:
            victim = next(n for n in cluster.ids if n != cluster.leader().id)
            cluster.crash(victim)
        if i == 9:
            cluster.restart(victim)
    read = cluster.submit(cluster.leader().id, "get", "k03")
    cluster.run(200)

    assert all(r["status"] == "ok" for r in records)
    assert read["result"] == "3"
    expected = [(f"k{i:02d}", str(i)) for i in range(12)]
    for machine in cluster.machines.values():
        assert [kv for kv in machine.store.scan("k", "l")] == expected
    # the restarted node resumed from its stored applied index, not from zero
    assert cluster.machines[victim].last_applied == max(cluster.applied)


def run_chaos(seed, node_class=RaftNode, ticks=700, cluster_class=Cluster):
    """Random partitions, crashes, restarts and client traffic, then heal and
    check convergence. Safety violations raise from inside the simulator."""
    rng = random.Random(seed)
    cluster = cluster_class(size=rng.choice([3, 5]), seed=seed, drop_rate=0.05,
                      duplicate_rate=0.05, delay=(1, 4), node_class=node_class)
    keys = ["x", "y", "z"]
    for _ in range(ticks):
        roll = rng.random()
        if roll < 0.010:
            shuffled = rng.sample(cluster.ids, len(cluster.ids))
            cut = rng.randint(1, len(shuffled) - 1)
            cluster.partition(shuffled[:cut], shuffled[cut:])
        elif roll < 0.020:
            cluster.heal()
        elif roll < 0.028 and cluster.nodes:
            cluster.crash(rng.choice(sorted(cluster.nodes)))
        elif roll < 0.040:
            cluster.restart(rng.choice(cluster.ids))
        if rng.random() < 0.12:
            # any node that believes it is leader may be asked, including stale ones
            believers = sorted(i for i, n in cluster.nodes.items() if n.role == LEADER)
            if believers:
                kind = rng.choice(["put", "put", "get", "get", "delete"])
                cluster.submit(rng.choice(believers), kind, rng.choice(keys),
                               str(rng.randrange(10**6)))
        cluster.step()

    cluster.heal()
    cluster.drop_rate = cluster.duplicate_rate = 0.0
    for node_id in cluster.ids:
        cluster.restart(node_id)
    target = lambda: (cluster.leader() is not None and len(
        {n.last_applied for n in cluster.nodes.values()}) == 1 and all(
        n.last_applied == n.storage.last_index() for n in cluster.nodes.values()))
    assert cluster.run_until(target, max_ticks=3000), "cluster did not converge after healing"
    cluster.check_log_matching()
    states = {tuple(sorted(m.data.items())) for m in cluster.machines.values()}
    assert len(states) == 1, "state machines diverged"
    ok, key = check_linearizable(cluster.history)
    if not ok:
        raise SafetyViolation(f"history for key {key!r} is not linearizable")
    return cluster


@pytest.mark.parametrize("seed", range(150))
def test_safety_and_linearizability_under_random_faults(seed):
    run_chaos(seed)


def test_chaos_runs_make_progress_and_exercise_faults():
    """Guards against a vacuous chaos test: operations must complete, some must
    be lost to faults, and leadership must change hands."""
    statuses = {"ok": 0, "unknown": 0, "failed": 0}
    terms = 0
    for seed in range(20):
        cluster = run_chaos(seed)
        for op in cluster.history:
            statuses[op["status"]] += 1
        terms += max(cluster.leaders_by_term)
    assert statuses["ok"] > 400
    assert statuses["unknown"] > 20 and statuses["failed"] > 5
    assert terms / 20 > 5


class VotesWithoutLogCheck(RaftNode):
    """Bug: grants votes to candidates with stale logs (drops section 5.4.1)."""
    def _log_up_to_date(self, last_term, last_index):
        return True


class CommitsWithoutMajority(RaftNode):
    """Bug: treats a single replica as a quorum."""
    def _quorum(self):
        return 1


class ServesReadsLocally(Cluster):
    """Bug: a node that believes it is leader answers reads from its own state
    without going through the log, so a deposed leader serves stale data."""
    def submit(self, node_id, op, key, value=None):
        node = self.nodes.get(node_id)
        if op == "get" and node is not None and node.role == LEADER:
            record = {"id": None, "op": "get", "key": key, "value": None,
                      "invoke": self.now, "return": self.now,
                      "result": self.machines[node_id].data.get(key), "status": "ok"}
            self.history.append(record)
            return record
        return super().submit(node_id, op, key, value)


@pytest.mark.parametrize("variant", [
    {"node_class": VotesWithoutLogCheck},
    {"node_class": CommitsWithoutMajority},
    {"cluster_class": ServesReadsLocally},
], ids=["votes-without-log-check", "commits-without-majority", "local-stale-reads"])
def test_the_harness_detects_known_bugs(variant):
    """The chaos test is only evidence if it fails on broken implementations."""
    caught = 0
    for seed in range(60):
        try:
            run_chaos(seed, **variant)
        except AssertionError:
            caught += 1
    assert caught >= 3, f"only {caught} of 60 seeds exposed the bug"
