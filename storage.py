import json
import os

import raft_pb2 as pb


def _atomic_write(path, content):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _to_line(entry):
    return json.dumps(
        {"index": entry.index, "term": entry.term, "key": entry.key, "value": entry.value}
    ) + "\n"


class Storage:
    """Persistência em disco de um nó: estado (termo/voto), log e banco comitado."""

    def __init__(self, directory):
        os.makedirs(directory, exist_ok=True)
        self.state_path = os.path.join(directory, "state.json")
        self.log_path = os.path.join(directory, "log.jsonl")
        self.db_path = os.path.join(directory, "db.json")

    def load_state(self):
        if not os.path.exists(self.state_path):
            return 0, 0
        with open(self.state_path, encoding="utf-8") as f:
            data = json.load(f)
        return data["term"], data["voted_for"]

    def save_state(self, term, voted_for):
        _atomic_write(self.state_path, json.dumps({"term": term, "voted_for": voted_for}))

    def load_log(self):
        if not os.path.exists(self.log_path):
            return []
        entries = []
        with open(self.log_path, encoding="utf-8") as f:
            for line in f:
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    break
                entries.append(pb.Entry(index=d["index"], term=d["term"], key=d["key"], value=d["value"]))
        return entries

    def append_log(self, entries):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.writelines(_to_line(e) for e in entries)
            f.flush()
            os.fsync(f.fileno())

    def rewrite_log(self, entries):
        _atomic_write(self.log_path, "".join(_to_line(e) for e in entries))

    def load_db(self):
        if not os.path.exists(self.db_path):
            return {}, 0
        with open(self.db_path, encoding="utf-8") as f:
            data = json.load(f)
        return data["data"], data["last_applied"]

    def save_db(self, data, last_applied):
        _atomic_write(self.db_path, json.dumps({"last_applied": last_applied, "data": data}))
