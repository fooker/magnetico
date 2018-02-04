# coding=utf-8
DEFAULT_MAX_METADATA_SIZE = 10 * 1024 * 1024
BOOTSTRAPPING_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("router.utorrent.com", 6881),
    ("dht.aelitis.com", 6881)
]
BOOTSTRAPPING_NODES6 = [
    ("dht.transmissionbt.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("router.silotis.us", 6881)
]

# threshold for pending info hashes before being committed to database:
PENDING_INFO_HASHES = 25

# seconds to pass after forcing commit to database
COMMIT_INTERVAL = 60

TICK_INTERVAL = 1  # in seconds

# maximum (inclusive) number of active (disposable) peers to fetch the
# metadata per info hash at the same time:
MAX_ACTIVE_PEERS_PER_INFO_HASH = 5

PEER_TIMEOUT = 120  # seconds

# database timeout backoff settings
MAX_BACKOFF_TIME = 64
