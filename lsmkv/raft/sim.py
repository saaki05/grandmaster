"""Deterministic cluster simulator.

One seeded random generator drives everything: election timeouts, message
delay, loss and duplication, partitions, crashes and client requests. A failing
seed therefore replays exactly. Time is a tick counter; nothing here uses
threads, sockets or the wall clock.

The simulator checks Raft's safety properties while it runs:
  * election safety      - at most one leader per term
  * state machine safety - no two nodes apply different entries at one index
and `check_log_matching()` verifies the log matching property on demand.
Client operations are recorded as a history for the linearizability checker.
"""
import heapq
import math
import random

from .node import LEADER, RaftNode
from .statemachine import DictStateMachine
from .storage import MemoryStorage


class SafetyViolation(AssertionError):
    pass


class Cluster:
    def __init__(self, size=3, seed=0, drop_rate=0.0, duplicate_rate=0.0,
                 delay=(1, 3), node_class=RaftNode, storage_factory=MemoryStorage,
                 state_machine_factory=None):
        self.rng = random.Random(seed)
        self.ids = [f"n{i}" for i in range(size)]
        self.drop_rate = drop_rate
        self.duplicate_rate = duplicate_rate
        self.delay = delay
        self.node_class = node_class
        self._sm_factory = state_machine_factory or (lambda node_id: DictStateMachine())
        self.storages = {i: storage_factory() for i in self.ids}
        self.nodes = {}
        self.machines = {}
        self.now = 0
        self._queue = []  # (deliver_at, sequence, destination, message)
        self._seq = 0
        self._blocked = set()  # frozenset({a, b}) pairs that cannot talk
        self.leaders_by_term = {}
        self.applied = {}  # index -> (term, command id) first applied anywhere
        self.history = []  # client operations, see _record
        self._pending = {}  # (node, index) -> operation
        self._op_ids = 0
        self.stats = {"sent": 0, "dropped": 0, "delivered": 0}
        for node_id in self.ids:
            self.start(node_id)

    # ----- node lifecycle -------------------------------------------------
    def start(self, node_id):
        machine = self._sm_factory(node_id)
        self.machines[node_id] = machine

        def apply(index, entry, node_id=node_id, machine=machine):
            self._on_apply(node_id, machine, index, entry)

        self.nodes[node_id] = self.node_class(
            node_id, self.ids, self.storages[node_id], apply,
            random.Random(self.rng.random()))

    def crash(self, node_id):
        """Lose all volatile state. Storage (the node's disk) is kept."""
        self.nodes.pop(node_id, None)
        machine = self.machines.pop(node_id, None)
        if hasattr(machine, "close"):
            machine.close()
        for key in [k for k in self._pending if k[0] == node_id]:
            self._pending.pop(key)  # outcome unknown: the client never hears back

    def restart(self, node_id):
        if node_id not in self.nodes:
            self.start(node_id)

    # ----- network ---------------------------------------------------------
    def partition(self, *groups):
        """Split the cluster into groups that cannot reach each other."""
        self._blocked = set()
        for a in self.ids:
            for b in self.ids:
                if a < b and not any(a in g and b in g for g in groups):
                    self._blocked.add(frozenset((a, b)))

    def heal(self):
        self._blocked = set()

    def _enqueue(self, src, dst, message):
        self.stats["sent"] += 1
        copies = 2 if self.rng.random() < self.duplicate_rate else 1
        for _ in range(copies):
            if self.rng.random() < self.drop_rate:
                self.stats["dropped"] += 1
                continue
            self._seq += 1
            at = self.now + self.rng.randint(*self.delay)
            heapq.heappush(self._queue, (at, self._seq, src, dst, message))

    def _flush_outboxes(self):
        for node_id, node in self.nodes.items():
            for dst, message in node.outbox:
                self._enqueue(node_id, dst, message)
            node.outbox.clear()

    # ----- time -------------------------------------------------------------
    def step(self):
        self.now += 1
        while self._queue and self._queue[0][0] <= self.now:
            _, _, src, dst, message = heapq.heappop(self._queue)
            if frozenset((src, dst)) in self._blocked or dst not in self.nodes:
                self.stats["dropped"] += 1
                continue
            self.stats["delivered"] += 1
            self.nodes[dst].step(dict(message))
            self._flush_outboxes()
        for node_id in list(self.nodes):
            self.nodes[node_id].tick()
        self._flush_outboxes()
        self._check_election_safety()

    def run(self, ticks):
        for _ in range(ticks):
            self.step()

    def run_until(self, predicate, max_ticks=2000):
        for _ in range(max_ticks):
            if predicate():
                return True
            self.step()
        return predicate()

    # ----- observation -------------------------------------------------------
    def leader(self):
        """The leader with the highest term among running nodes, if any."""
        leaders = [n for n in self.nodes.values() if n.role == LEADER]
        return max(leaders, key=lambda n: n.term) if leaders else None

    def _check_election_safety(self):
        for node in self.nodes.values():
            if node.role == LEADER:
                seen = self.leaders_by_term.setdefault(node.term, node.id)
                if seen != node.id:
                    raise SafetyViolation(
                        f"two leaders in term {node.term}: {seen} and {node.id}")

    def _on_apply(self, node_id, machine, index, entry):
        command = entry["cmd"]
        identity = (entry["term"], command["id"] if command else None)
        first = self.applied.setdefault(index, identity)
        if first != identity:
            raise SafetyViolation(
                f"{node_id} applied {identity} at index {index}, another node applied {first}")
        result = machine.apply(index, command)
        op = self._pending.pop((node_id, index), None)
        if op is not None:
            if command is not None and command["id"] == op["id"]:
                op["return"] = self.now
                op["result"] = result
                op["status"] = "ok"
            else:
                op["status"] = "failed"  # a different entry took this index

    def check_log_matching(self):
        nodes = list(self.storages.items())
        for i, (a_id, a) in enumerate(nodes):
            for b_id, b in nodes[i + 1:]:
                common = min(a.last_index(), b.last_index())
                agree_from = None
                for index in range(common, 0, -1):
                    if a.term_at(index) == b.term_at(index):
                        agree_from = index
                        break
                if agree_from is None:
                    continue
                for index in range(1, agree_from + 1):
                    if a.entry(index) != b.entry(index):
                        raise SafetyViolation(
                            f"log matching violated between {a_id} and {b_id} at {index}")

    # ----- clients ------------------------------------------------------------
    def submit(self, node_id, op, key, value=None):
        """Send one client command to `node_id`. Returns the recorded operation,
        or None if that node is down or does not believe it is leader."""
        node = self.nodes.get(node_id)
        if node is None or node.role != LEADER:
            return None
        self._op_ids += 1
        command = {"id": self._op_ids, "op": op, "key": key}
        if op == "put":
            command["value"] = value
        record = {"id": self._op_ids, "op": op, "key": key, "value": value,
                  "invoke": self.now, "return": math.inf, "result": None,
                  "status": "unknown"}
        self.history.append(record)
        # registered before proposing: a single-node cluster applies immediately
        self._pending[(node_id, node.storage.last_index() + 1)] = record
        node.propose(command)
        self._flush_outboxes()
        return record
