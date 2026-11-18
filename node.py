import argparse
import os
import random
import threading
import time
from concurrent import futures

import grpc

import raft_pb2 as pb
import raft_pb2_grpc as rpc
from cluster import NODE_IDS, QUORUM, address
from storage import Storage

FOLLOWER, CANDIDATE, LEADER = "SEGUIDOR", "CANDIDATO", "LIDER"
MAX_BATCH = 100
RPC_TIMEOUT = 0.3
STATUS_PERIOD = 2.0
CHANNEL_OPTIONS = (
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
)


class Node(rpc.RaftServicer, rpc.ClientServicer):
    """Nó do cluster: alterna entre Seguidor, Candidato e Líder."""

    def __init__(self, node_id, fetch_ms, election_min_ms, election_max_ms, hb_timeout_ms, commit_wait_s, data_dir):
        self.id = node_id
        self.peers = [p for p in NODE_IDS if p != node_id]
        self.fetch_s = fetch_ms / 1000
        self.election_range = (election_min_ms / 1000, election_max_ms / 1000)
        self.hb_timeout = hb_timeout_ms / 1000
        self.commit_wait = commit_wait_s

        self.store = Storage(os.path.join(data_dir, f"node{node_id}"))
        self.term, self.voted_for = self.store.load_state()
        self.log = self.store.load_log()
        self.db, self.last_applied = self.store.load_db()
        self.commit_index = self.last_applied

        self.role = FOLLOWER
        self.leader_id = None
        self.votes = set()
        self.last_fetch = {}
        self.match = {}

        self.lock = threading.RLock()
        self.commit_cv = threading.Condition(self.lock)
        self.stubs = {p: rpc.RaftStub(grpc.insecure_channel(address(p), options=CHANNEL_OPTIONS)) for p in self.peers}
        self.pool = futures.ThreadPoolExecutor(max_workers=len(self.peers))
        self.deadline = 0.0
        self._reset_deadline()

    def _say(self, msg):
        print(f"[N{self.id}] {msg}", flush=True)

    def _status_line(self):
        return (
            f"papel={self.role} termo={self.term} commit={self.commit_index} "
            f"ultimo_indice={len(self.log)} lider={self.leader_id}"
        )

    def _last_index(self):
        return len(self.log)

    def _term_at(self, index):
        return 0 if index == 0 else self.log[index - 1].term

    def _reset_deadline(self):
        self.deadline = time.monotonic() + random.uniform(*self.election_range)

    def _become_follower(self, term, leader_id=None):
        if term > self.term:
            self.term = term
            self.voted_for = 0
            self.store.save_state(self.term, self.voted_for)
        changed = self.role != FOLLOWER
        self.role = FOLLOWER
        self.leader_id = leader_id
        self.commit_cv.notify_all()
        if changed:
            self._say(f"virou SEGUIDOR | {self._status_line()}")

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.id
        self.last_fetch = {}
        self.match = {p: 0 for p in self.peers}
        noop = pb.Entry(index=self._last_index() + 1, term=self.term, key="", value="")
        self.log.append(noop)
        self.store.append_log([noop])
        self._say(f"virou LIDER | {self._status_line()}")

    def _apply_up_to(self, index):
        if index <= self.last_applied:
            return
        for entry in self.log[self.last_applied:index]:
            if entry.key:
                self.db[entry.key] = entry.value
        self.last_applied = index
        self.store.save_db(self.db, self.last_applied)
        self.commit_index = index
        self.commit_cv.notify_all()
        self._say(f"commit avancou | {self._status_line()}")

    def _advance_commit(self):
        indexes = sorted([self._last_index()] + [self.match[p] for p in self.peers], reverse=True)
        candidate = indexes[QUORUM - 1]
        if candidate > self.commit_index and self._term_at(candidate) == self.term:
            self._apply_up_to(candidate)

    def _active_count(self):
        now = time.monotonic()
        return 1 + sum(1 for p in self.peers if now - self.last_fetch.get(p, float("-inf")) <= self.hb_timeout)

    def _truncate(self, to_index):
        if to_index < self.commit_index:
            self._say(f"ERRO: truncamento para {to_index} abaixo do commit {self.commit_index} ignorado")
            return
        removed = len(self.log) - to_index
        self.log = self.log[:to_index]
        self.store.rewrite_log(self.log)
        self._say(f"log truncado em {to_index} ({removed} entrada(s) removida(s)) | {self._status_line()}")

    def RequestVote(self, req, ctx):
        with self.lock:
            if req.term > self.term:
                self._become_follower(req.term)
            up_to_date = (req.last_log_term, req.last_log_index) >= (self._term_at(self._last_index()), self._last_index())
            granted = req.term == self.term and self.voted_for in (0, req.candidate_id) and up_to_date
            if granted:
                self.voted_for = req.candidate_id
                self.store.save_state(self.term, self.voted_for)
                self._reset_deadline()
            self._say(f"voto para N{req.candidate_id} no termo {req.term}: {'CONCEDIDO' if granted else 'NEGADO'}")
            return pb.VoteResponse(term=self.term, vote_granted=granted)

    def Fetch(self, req, ctx):
        with self.lock:
            if req.term > self.term:
                self._become_follower(req.term)
            if self.role != LEADER or req.term < self.term:
                return pb.FetchResponse(term=self.term, success=False, leader_id=self.leader_id or 0)

            replica = req.replica_id
            self.last_fetch[replica] = time.monotonic()
            last_index = self._last_index()
            if req.last_log_index > last_index or self._term_at(req.last_log_index) != req.last_log_term:
                self.match[replica] = 0
                truncate_to = min(req.last_log_index, last_index)
                while truncate_to > 0 and self._term_at(truncate_to) > req.last_log_term:
                    truncate_to -= 1
                return pb.FetchResponse(
                    term=self.term, success=True, leader_id=self.id,
                    commit_index=self.commit_index, diverged=True, truncate_to=truncate_to,
                )

            self.match[replica] = req.last_log_index
            self._advance_commit()
            batch = self.log[req.last_log_index:req.last_log_index + MAX_BATCH]
            return pb.FetchResponse(
                term=self.term, success=True, leader_id=self.id,
                entries=batch, commit_index=self.commit_index,
            )

    def Write(self, req, ctx):
        with self.lock:
            if self.role != LEADER:
                return pb.WriteResponse(ok=False, leader_id=self.leader_id or 0, message=f"N{self.id} nao e o lider ({self.role})")
            if not req.key:
                return pb.WriteResponse(ok=False, leader_id=self.id, message="chave vazia")
            active = self._active_count()
            if active < QUORUM:
                return pb.WriteResponse(
                    ok=False, leader_id=self.id,
                    message=f"sem quorum: {active} no(s) ativo(s), minimo {QUORUM}",
                )
            entry = pb.Entry(index=self._last_index() + 1, term=self.term, key=req.key, value=req.value)
            self.log.append(entry)
            self.store.append_log([entry])
            self._say(f"entrada {entry.index} (termo {entry.term}) gravada no log, aguardando commit")
            self.commit_cv.wait_for(
                lambda: self.role != LEADER or self.term != entry.term or self.commit_index >= entry.index,
                timeout=self.commit_wait,
            )
            if entry.index <= self.commit_index and self._term_at(entry.index) == entry.term:
                return pb.WriteResponse(ok=True, leader_id=self.id, index=entry.index, message="comitado")
            return pb.WriteResponse(
                ok=False, leader_id=self.id, index=entry.index,
                message="entrada nao comitada (quorum nao confirmou a tempo ou lideranca perdida)",
            )

    def Read(self, req, ctx):
        with self.lock:
            found = req.key in self.db
            return pb.ReadResponse(found=found, value=self.db.get(req.key, ""))

    def Status(self, req, ctx):
        with self.lock:
            return pb.StatusResponse(
                node_id=self.id, role=self.role, term=self.term, commit_index=self.commit_index,
                last_index=self._last_index(), leader_id=self.leader_id or 0, voted_for=self.voted_for,
                log=self.log, db=self.db,
            )

    def _start_election(self):
        with self.lock:
            self.term += 1
            self.role = CANDIDATE
            self.voted_for = self.id
            self.leader_id = None
            self.votes = {self.id}
            self.store.save_state(self.term, self.voted_for)
            self._reset_deadline()
            req = pb.VoteRequest(
                term=self.term, candidate_id=self.id,
                last_log_index=self._last_index(), last_log_term=self._term_at(self._last_index()),
            )
            self._say(f"virou CANDIDATO, iniciando eleicao | {self._status_line()}")
        for peer in self.peers:
            self.pool.submit(self._ask_vote, peer, req)

    def _ask_vote(self, peer, req):
        try:
            resp = self.stubs[peer].RequestVote(req, timeout=RPC_TIMEOUT)
        except grpc.RpcError:
            return
        with self.lock:
            if resp.term > self.term:
                self._become_follower(resp.term)
                return
            if self.role != CANDIDATE or self.term != req.term or not resp.vote_granted:
                return
            self.votes.add(peer)
            if len(self.votes) >= QUORUM:
                self._become_leader()

    def _on_fetch_response(self, req, resp):
        with self.lock:
            if resp.term > self.term:
                self._become_follower(resp.term, resp.leader_id or None)
                return
            if resp.term < self.term or req.term != self.term or self.role != FOLLOWER:
                return
            self.leader_id = resp.leader_id or None
            if not resp.success:
                return
            self._reset_deadline()
            if resp.diverged:
                self._truncate(resp.truncate_to)
                return
            appended = []
            for entry in resp.entries:
                if entry.index == self._last_index() + 1:
                    self.log.append(entry)
                    appended.append(entry)
            if appended:
                self.store.append_log(appended)
                self._say(f"recebeu {len(appended)} entrada(s) (ate indice {self._last_index()}) | {self._status_line()}")
            self._apply_up_to(min(resp.commit_index, self._last_index()))

    def fetch_loop(self):
        rotation = 0
        while True:
            time.sleep(self.fetch_s)
            with self.lock:
                if self.role != FOLLOWER:
                    continue
                target = self.leader_id
                if target is None:
                    target = self.peers[rotation % len(self.peers)]
                    rotation += 1
                req = pb.FetchRequest(
                    term=self.term, replica_id=self.id,
                    last_log_index=self._last_index(), last_log_term=self._term_at(self._last_index()),
                )
            try:
                resp = self.stubs[target].Fetch(req, timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                with self.lock:
                    if self.leader_id == target and self.term == req.term:
                        self.leader_id = None
                continue
            self._on_fetch_response(req, resp)

    def election_loop(self):
        while True:
            time.sleep(0.02)
            with self.lock:
                due = self.role != LEADER and time.monotonic() >= self.deadline
            if due:
                self._start_election()

    def status_loop(self):
        while True:
            time.sleep(STATUS_PERIOD)
            with self.lock:
                self._say(self._status_line())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=NODE_IDS)
    parser.add_argument("--fetch-ms", type=int, default=100)
    parser.add_argument("--election-min-ms", type=int, default=600)
    parser.add_argument("--election-max-ms", type=int, default=1200)
    parser.add_argument("--hb-timeout-ms", type=int, default=500)
    parser.add_argument("--commit-wait-s", type=float, default=3.0)
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    args = parser.parse_args()

    node = Node(args.id, args.fetch_ms, args.election_min_ms, args.election_max_ms,
                args.hb_timeout_ms, args.commit_wait_s, args.data_dir)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    rpc.add_RaftServicer_to_server(node, server)
    rpc.add_ClientServicer_to_server(node, server)
    server.add_insecure_port(f"0.0.0.0:{address(args.id).split(':')[1]}")
    server.start()
    node._say(f"iniciado | {node._status_line()}")

    for loop in (node.fetch_loop, node.election_loop, node.status_loop):
        threading.Thread(target=loop, daemon=True).start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
