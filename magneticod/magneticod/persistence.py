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
import typing
from datetime import datetime
from functools import lru_cache

import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import DocType, InnerObjectWrapper, Date, Long, \
    Text, Nested

from magneticod import bencode


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


class Database:
    def __init__(self, database) -> None:
        self.elastic = Elasticsearch(hosts=['192.168.200.1'], timeout=15)
        Torrent.init(using=self.elastic)

        # We buffer metadata to flush many entries at once, for performance
        # reasons.
        # list of tuple (info_hash, name, total_size, discovered_on)
        self.__pending_metadata = []  # type: typing.List[typing.Tuple[bytes,
        #  str, int, int]]
        # list of tuple (info_hash, size, path)
        self.__pending_files = []  # type: typing.List[typing.Tuple[bytes,
        # int, bytes]]

    def add_metadata(self, info_hash: bytes, metadata: bytes) -> bool:
        torrent = Torrent()
        torrent.meta.id = info_hash.hex()
        torrent.found_at = datetime.utcnow()
        torrent.found_by = socket.gethostname()
        torrent.size = 0
        try:
            info = bencode.loads(metadata)

            assert b"/" not in info[b"name"]
            torrent.name = info[b"name"].decode("utf-8")

            if b"files" in info:  # Multiple File torrent:
                for file in info[b"files"]:
                    assert type(file[b"length"]) is int
                    # Refuse trailing slash in any of the path items
                    assert not any(b"/" in item for item in file[b"path"])
                    path = "/".join(i.decode("utf-8") for i in file[b"path"])
                    torrent.files.append(
                        {'size': file[b"length"], 'path': path})
                    torrent.size += file[b"length"]
            else:  # Single File torrent:
                assert type(info[b"length"]) is int
                torrent.files.append(
                    {'size': info[b"length"], 'path': torrent.name})
                torrent.size = info[b"length"]
        # TODO: Make sure this catches ALL, AND ONLY operational errors
            assert (torrent.size != 0)
        except (
        bencode.BencodeDecodingError, AssertionError, KeyError, AttributeError,
        UnicodeDecodeError, TypeError):
            logging.error("exception")
            return False

        logging.info("Added: `%s` (%s)", torrent.name, torrent.meta.id)

        torrent.save(using=self.elastic)

        logging.info(self.is_infohash_new.cache_info())

        return True

    @lru_cache(maxsize=4096)
    def is_infohash_new(self, info_hash):
        try:
            self.elastic.get(index='torrents', id=info_hash.hex())
        except elasticsearch.exceptions.NotFoundError:
            return True

        return False

    def close(self) -> None:
        pass
