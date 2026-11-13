"""A single Raft node as a pure, deterministic state machine.

The node does no I/O of its own beyond its storage object. A driver calls
`tick()` to advance logical time and `step(message)` to deliver a message, then
drains `outbox` and sends those messages however it likes. That makes the same
code usable under a real transport and under the seeded simulator in
`lsmkv.raft.sim`, where every run is reproducible.

Implemented: leader election, log replication, commitment, a no-op entry on
election. Not implemented: snapshots / log compaction, membership changes,
read leases (reads go through the log).
"""

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class RaftNode:
    def __init__(self, node_id, peers, storage, apply_fn, rng,
                 election_timeout=(10, 20), heartbeat_interval=3):
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.storage = storage
        self.apply_fn = apply_fn  # called as apply_fn(index, entry) for each committed entry
        self.rng = rng
        self.election_timeout_range = election_timeout
        self.heartbeat_interval = heartbeat_interval

        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.outbox = []  # list of (destination, message)
        self._elapsed = 0
        self._reset_election_timer()

    # ----- helpers -------------------------------------------------------
    @property
    def term(self):
        return self.storage.term

    def _quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def _reset_election_timer(self):
        self._elapsed = 0
        self._timeout = self.rng.randint(*self.election_timeout_range)

    def _send(self, dest, message):
        message["term"] = self.term
        message["from"] = self.id
        self.outbox.append((dest, message))

    def _become_follower(self, term):
        if term != self.term:
            self.storage.save_state(term, None)
        self.role = FOLLOWER
        self.votes = set()

    def _log_up_to_date(self, last_term, last_index):
        """Section 5.4.1: the candidate's log must be at least as complete as ours."""
        my_last = self.storage.last_index()
        my_term = self.storage.term_at(my_last)
        return (last_term, last_index) >= (my_term, my_last)

    # ----- time ----------------------------------------------------------
    def tick(self):
        self._elapsed += 1
        if self.role == LEADER:
            if self._elapsed >= self.heartbeat_interval:
                self._broadcast_append()
        elif self._elapsed >= self._timeout:
            self._start_election()

    def _start_election(self):
        self.storage.save_state(self.term + 1, self.id)
        self.role = CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer()
        last = self.storage.last_index()
        for peer in self.peers:
            self._send(peer, {"type": "vote_request", "last_index": last,
                              "last_term": self.storage.term_at(last)})
        if len(self.votes) >= self._quorum():
            self._become_leader()

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.id
        last = self.storage.last_index()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A leader may only count replicas for entries of its own term
        # (section 5.4.2), so commit a no-op to commit everything before it.
        self.storage.append({"term": self.term, "cmd": None})
        self._advance_commit()
        self._broadcast_append()

    # ----- client entry point -------------------------------------------
    def propose(self, command):
        """Append a command if this node is leader. Returns its log index, or
        None if this node is not the leader. Acceptance is not commitment."""
        if self.role != LEADER:
            return None
        self.storage.append({"term": self.term, "cmd": command})
        index = self.storage.last_index()
        self._advance_commit()
        self._broadcast_append()
        return index

    # ----- replication ---------------------------------------------------
    def _broadcast_append(self):
        self._elapsed = 0
        for peer in self.peers:
            self._send_append(peer)

    def _send_append(self, peer):
        prev = self.next_index[peer] - 1
        self._send(peer, {
            "type": "append",
            "prev_index": prev,
            "prev_term": self.storage.term_at(prev),
            "entries": list(self.storage.entries_from(prev + 1)),
            "leader_commit": self.commit_index,
        })

    def _advance_commit(self):
        for n in range(self.storage.last_index(), self.commit_index, -1):
            if self.storage.term_at(n) != self.term:
                break  # never commit an earlier term's entry by counting replicas
            replicas = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if replicas >= self._quorum():
                self.commit_index = n
                break
        self._apply_committed()

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.apply_fn(self.last_applied, self.storage.entry(self.last_applied))

    # ----- message handling ---------------------------------------------
    def step(self, msg):
        if msg["term"] > self.term:
            self._become_follower(msg["term"])
        handler = getattr(self, "_on_" + msg["type"])
        handler(msg)

    def _on_vote_request(self, msg):
        granted = (
            msg["term"] == self.term
            and self.storage.voted_for in (None, msg["from"])
            and self._log_up_to_date(msg["last_term"], msg["last_index"])
        )
        if granted:
            self.storage.save_state(self.term, msg["from"])
            self._reset_election_timer()
        self._send(msg["from"], {"type": "vote_reply", "granted": granted})

    def _on_vote_reply(self, msg):
        if self.role != CANDIDATE or msg["term"] != self.term or not msg["granted"]:
            return
        self.votes.add(msg["from"])
        if len(self.votes) >= self._quorum():
            self._become_leader()

    def _on_append(self, msg):
        if msg["term"] < self.term:
            self._send(msg["from"], {"type": "append_reply", "success": False,
                                     "match_index": 0})
            return
        self.role = FOLLOWER
        self.leader_id = msg["from"]
        self._reset_election_timer()

        prev = msg["prev_index"]
        if prev > self.storage.last_index() or self.storage.term_at(prev) != msg["prev_term"]:
            # hint lets the leader skip back past our whole missing suffix at once
            hint = min(self.storage.last_index(), prev - 1)
            self._send(msg["from"], {"type": "append_reply", "success": False,
                                     "match_index": hint})
            return

        for offset, entry in enumerate(msg["entries"]):
            index = prev + 1 + offset
            if index <= self.storage.last_index():
                if self.storage.term_at(index) == entry["term"]:
                    continue
                self.storage.truncate_from(index)  # conflict: drop it and all that follow
            self.storage.append(entry)

        last_new = prev + len(msg["entries"])
        if msg["leader_commit"] > self.commit_index:
            self.commit_index = max(self.commit_index, min(msg["leader_commit"], last_new))
        self._apply_committed()
        self._send(msg["from"], {"type": "append_reply", "success": True,
                                 "match_index": last_new})

    def _on_append_reply(self, msg):
        if self.role != LEADER or msg["term"] != self.term:
            return
        peer = msg["from"]
        if msg["success"]:
            self.match_index[peer] = max(self.match_index[peer], msg["match_index"])
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
        else:
            self.next_index[peer] = max(1, min(self.next_index[peer] - 1,
                                               msg["match_index"] + 1))
            self._send_append(peer)
