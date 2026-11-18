NODE_IDS = (1, 2, 3, 4)
QUORUM = len(NODE_IDS) // 2 + 1
BASE_PORT = 50050


def address(node_id):
    return f"127.0.0.1:{BASE_PORT + node_id}"
