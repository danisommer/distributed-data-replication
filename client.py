import argparse
import sys
import time

import grpc

import raft_pb2 as pb
import raft_pb2_grpc as rpc
from cluster import NODE_IDS, address

MAX_ATTEMPTS = 10


def stub_for(node_id):
    return rpc.ClientStub(grpc.insecure_channel(address(node_id)))


def write(key, value):
    target = NODE_IDS[0]
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = stub_for(target).Write(pb.WriteRequest(key=key, value=value), timeout=10)
        except grpc.RpcError:
            print(f"N{target} indisponivel")
            target = NODE_IDS[(NODE_IDS.index(target) + 1) % len(NODE_IDS)]
            time.sleep(0.3)
            continue
        if resp.ok:
            print(f"OK: {key}={value} comitado no indice {resp.index} pelo lider N{resp.leader_id}")
            return 0
        if resp.leader_id == target:
            print(f"REJEITADO pelo lider N{target}: {resp.message}")
            return 1
        if resp.leader_id:
            print(f"N{target} nao e lider, redirecionando para N{resp.leader_id}")
            target = resp.leader_id
        else:
            print(f"N{target} desconhece o lider, tentando outro no")
            target = NODE_IDS[(NODE_IDS.index(target) + 1) % len(NODE_IDS)]
            time.sleep(0.3)
    print("FALHA: lider nao encontrado")
    return 1


def read(key, nodes):
    code = 0
    for node_id in nodes:
        try:
            resp = stub_for(node_id).Read(pb.ReadRequest(key=key), timeout=2)
        except grpc.RpcError:
            print(f"N{node_id}: indisponivel")
            code = 1
            continue
        print(f"N{node_id}: {key}={resp.value}" if resp.found else f"N{node_id}: {key} nao encontrado (apenas dados comitados)")
    return code


def status(nodes):
    code = 0
    for node_id in nodes:
        try:
            s = stub_for(node_id).Status(pb.StatusRequest(), timeout=2)
        except grpc.RpcError:
            print(f"N{node_id}: indisponivel")
            code = 1
            continue
        print(
            f"N{s.node_id}: papel={s.role} termo={s.term} commit={s.commit_index} "
            f"ultimo_indice={s.last_index} lider={s.leader_id or None} voto={s.voted_for or None}"
        )
        for e in s.log:
            mark = "comitada" if e.index <= s.commit_index else "NAO comitada"
            print(f"    log[{e.index}] termo={e.term} {e.key or '(noop)'}={e.value}  [{mark}]")
        print(f"    banco: {dict(s.db)}")
    return code


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write")
    w.add_argument("key")
    w.add_argument("value")
    r = sub.add_parser("read")
    r.add_argument("key")
    r.add_argument("--node", type=int, choices=NODE_IDS)
    s = sub.add_parser("status")
    s.add_argument("--node", type=int, choices=NODE_IDS)
    args = parser.parse_args()

    match args.cmd:
        case "write":
            return write(args.key, args.value)
        case "read":
            return read(args.key, [args.node] if args.node else NODE_IDS)
        case "status":
            return status([args.node] if args.node else NODE_IDS)


if __name__ == "__main__":
    sys.exit(main())
