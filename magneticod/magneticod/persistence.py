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
import time
from datetime import datetime

import asyncio
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import DocType, Date, Long, \
    Nested, Keyword, Text

from magneticod import bencode
from magneticod.constants import PENDING_INFO_HASHES, MAX_BACKOFF_TIME, COMMIT_INTERVAL
from magneticod.cache import RedisLRUCache, LRUDictCache

logging.getLogger('elasticsearch').setLevel(logging.ERROR)


class Torrent(DocType):
    name = Text(fields={'keyword': Keyword(ignore_above=4096)})
    size = Long()
    found_at = Date()
    found_by = Keyword()

    files = Nested(properties={
        'path': Text(fields={'keyword': Keyword(ignore_above=4096)}),
        'size': Long(),
    })

    class Meta:
        index = 'torrents'
        doc_type = 'torrent'

    def __hash__(self):
        return hash(self.meta.id)


class Database:
    def __init__(self, hosts, redis=None) -> None:
        logging.info("elasticsearch via {}".format(', '.join(hosts)))
        self.elastic = Elasticsearch(
            hosts=hosts, retry_on_timeout=True,
            #sniff_on_start=True, sniff_on_failure=True, sniffer_timeout=60
        )
        Torrent.init(using=self.elastic)

        if redis:
            self.cache = RedisLRUCache()
        else:
            self.cache = LRUDictCache()

        self.pending = set()
        self.commited = time.time()

        self.backoff_until = 0
        self.backoff_errors = 0

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
            #  logging.exception("Error during metadata decoding")
            return False

        self.pending.add(torrent)
        self.cache.put(info_hash)
        logging.info("Added: `%s` (%s)", torrent.name, torrent.meta.id)

        if len(self.pending) >= PENDING_INFO_HASHES or \
           time.time() - self.commited >= COMMIT_INTERVAL:
            asyncio.ensure_future(self.commit(frozenset(self.pending)))

        return True

    async def commit(self, torrents, ignore_backoff=False):
        if self.backoff_until > time.time() and not ignore_backoff:
            return

        logging.info("Committing %d torrents" % len(self.pending))
        try:
            bulk(self.elastic, (torrent.to_dict(True) for torrent in self.pending))
        except socket.timeout:
            self.backoff_errors += 1
            backoff_penalty = min(2 ** self.backoff_errors, MAX_BACKOFF_TIME)
            self.backoff_until = time.time() + backoff_penalty
            logging.info("db timeout #%d, waiting %ds before next retry" % (
                self.backoff_errors, backoff_penalty,
            ))
        self.pending = self.pending.difference(torrents)
        self.commited = time.time()
        self.backoff_errors = 0
        logging.info("Cache: Hit/Miss %d:%d (%.1f%%)" % self.cache.stats())

    def infohash_exists(self, info_hash):
        # query cache
        if self.cache.exists(info_hash):
            return True

        # query database
        exists = self.elastic.exists(
            index='torrents', id=info_hash.hex(), doc_type='torrent'
        )
        if exists:
            # update cache if infohash exists
            self.cache.put(info_hash)

        return exists

    async def close(self) -> None:
        if len(self.pending) > 0:
            await self.commit(self.pending, ignore_backoff=True)
