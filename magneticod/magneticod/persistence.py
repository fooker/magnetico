# magneticod - Autonomous BitTorrent DHT crawler and metadata fetcher.
# Copyright (C) 2017  Mert Bora ALPER <bora@boramalper.org>
# Dedicated to Cemile Binay, in whose hands I thrived.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General
# Public License as published by the Free Software Foundation, either version
#  3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
#  ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
import logging
import socket
from datetime import datetime

import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import DocType, InnerObjectWrapper, Date, Long, \
    Text, Nested
from elasticsearch.helpers import bulk
from lru import LRU

from magneticod import bencode
from magneticod.constants import PENDING_INFO_HASHES

logging.getLogger('elasticsearch').setLevel(logging.ERROR)


class File(InnerObjectWrapper):
    path = Text()
    size = Long()


class Torrent(DocType):
    name = Text()
    size = Long()
    found_at = Date()
    found_by = Text()

    files = Nested(doc_class=File)

    class Meta:
        index = 'torrents'
        doc_type = 'torrent'


class Database:
    def __init__(self, hosts) -> None:
        logging.info("elasticsearch via {}".format(', '.join(hosts)))
        self.elastic = Elasticsearch(hosts=hosts, timeout=15)
        Torrent.init(using=self.elastic)

        self.infohash_lru = LRU(2**14)
        self.lru_hit = 0
        self.lru_miss = 0

        self.pending = []

    def add_metadata(self, info_hash: bytes, metadata: bytes) -> bool:
        torrent = Torrent()
        torrent.meta.id = info_hash.hex()
        torrent.found_at = datetime.utcnow()
        torrent.found_by = socket.gethostname()
        torrent.size = 0
        try:
            info = bencode.loads(metadata)

            assert b'/' not in info[b'name']
            torrent.name = info[b'name'].decode("utf-8")

            if b'files' in info:  # Multiple File torrent:
                for file in info[b'files']:
                    assert type(file[b'length']) is int
                    # Refuse trailing slash in any of the path items
                    assert not any(b"/" in item for item in file[b'path'])
                    path = '/'.join(i.decode('utf-8') for i in file[b'path'])
                    torrent.files.append(
                        {'size': file[b'length'], 'path': path})
                    torrent.size += file[b'length']
            else:  # Single File torrent:
                assert type(info[b'length']) is int
                torrent.files.append(
                    {'size': info[b'length'], 'path': torrent.name})
                torrent.size = info[b'length']
                # TODO: Make sure this catches ALL, AND ONLY operational errors
            assert (torrent.size != 0)
        except (
                bencode.BencodeDecodingError, AssertionError, KeyError,
                AttributeError,
                UnicodeDecodeError, TypeError) as ex:
            logging.exception("Error during metadata decoding")
            return False

        self.pending.append(torrent)
        self.infohash_lru[info_hash] = False
        logging.info("Added: `%s` (%s)", torrent.name, torrent.meta.id)

        if len(self.pending) >= PENDING_INFO_HASHES:
            self.commit()

        return True

    def commit(self):
        logging.info("Committing %d torrents" % len(self.pending))
        bulk(self.elastic, (torrent.to_dict(True) for torrent in self.pending))
        self.pending.clear()
        stats = self.infohash_lru.get_stats() + (len(self.infohash_lru.keys()), self.infohash_lru.get_size())
        logging.info("Infohash LRU-Cache (Hit: {}, Miss: {}, Fullness: {}/{})".format(*stats))

    def is_infohash_new(self, info_hash):
        try:
            return self.infohash_lru[info_hash]
        except KeyError:
            pass

        exists = self.elastic.exists(index='torrents', id=info_hash.hex(), doc_type='torrent')
        self.infohash_lru[info_hash] = exists
        return exists

    def close(self) -> None:
        pass
