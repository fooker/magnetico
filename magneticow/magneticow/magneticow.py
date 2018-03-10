# magneticow - Lightweight web interface for magnetico.
# Copyright (C) 2017  Mert Bora ALPER <bora@boramalper.org>
# Dedicated to Cemile Binay, in whose hands I thrived.
#
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
import collections
import datetime as dt
from datetime import datetime
import logging

import flask

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from magneticow import utils
from magneticow.authorization import requires_auth, generate_feed_hash


File = collections.namedtuple("file", ["path", "size"])
Torrent = collections.namedtuple("torrent", ["info_hash", "name", "size", "found_at", "files"])


app = flask.Flask(__name__)
app.config.from_object(__name__)

# TODO: We should have been able to use flask.g but it does NOT persist across different requests so we resort back to
# this. Investigate the cause and fix it (I suspect of Gevent).
magneticod_db = None

@app.route("/")
@requires_auth
def home_page():
    n_torrents = Search(using=magneticod_db).count()

    return flask.render_template("homepage.html", n_torrents=n_torrents)


@app.route("/torrents/")
@requires_auth
def torrents():
    search = flask.request.args.get("search")
    page = int(flask.request.args.get("page", 0))

    context = {
        "search": search,
        "page": page
    }

    q = Search(using=magneticod_db)

    if search:
        q = q.query("simple_query_string",
                    query=search,
                    fields=["name.search", "file.path.search"],
                    default_operator="and")

    sort_by = flask.request.args.get("sort_by")
    allowed_sorts = [
        None,
        "name asc",
        "name desc",
        "size asc",
        "size desc",
        "found_at asc",
        "found_at desc"
    ]
    if sort_by not in allowed_sorts:
        return flask.Response("Invalid value for `sort_by! (Allowed values are %s)" % (allowed_sorts, ), 400)

    if sort_by:
        sort_by_field, sort_by_dir = sort_by.split(' ')
    else:
        sort_by_field, sort_by_dir = None, None

    if sort_by_field == "name":
        sort_by_field = "name.keyword"

    if search:
        if sort_by_field:
            q = q.sort({sort_by_field: {'order': sort_by_dir}},
                       {'_score': {'order': 'asc'}})
        else:
            q = q.sort({'_score': {'order': 'asc'}})
    else:
        if sort_by_field:
            q = q.sort({sort_by_field: {'order': sort_by_dir}})

    q = q[20 * (page + 0):20 * (page + 1)]

    result = q.execute()

    context["torrents"] = [Torrent(t.meta.id, t.name, utils.to_human_size(t.size),
                                   t.found_at, [])
                           for t in result.hits]

    if len(context["torrents"]) < 20:
        context["next_page_exists"] = False
    else:
        context["next_page_exists"] = True

    if app.arguments.noauth:
        context["subscription_url"] = "/feed/?filter%s" % search
    else:
        username, password = flask.request.authorization.username, flask.request.authorization.password
        context["subscription_url"] = "/feed?filter=%s&hash=%s" % (
            search, generate_feed_hash(username, password, search))

    if sort_by:
        context["sorted_by"] = sort_by

    return flask.render_template("torrents.html", **context)


@app.route("/torrents/<info_hash>", defaults={"name": None})
@requires_auth
def torrent(**kwargs):
    context = {}

    try:
        info_hash = bytes.fromhex(kwargs["info_hash"])
        assert len(info_hash) == 20
    except (AssertionError, ValueError):  # In case info_hash variable is not a proper hex-encoded bytes
        return flask.abort(400)

    try:
        r = magneticod_db.get(index="torrents", doc_type="torrent", id=info_hash.hex())
    except TypeError:  # In case no results returned, TypeError will be raised when we try to subscript None object
        return flask.abort(404)

    r = r["_source"]

    files = sorted(File(f["path"], utils.to_human_size(f["size"])) for f in r["files"])

    context["torrent"] = Torrent(info_hash=info_hash.hex(),
                                 name=r["name"],
                                 size=utils.to_human_size(r["size"]),
                                 found_at=r["found_at"],
                                 files=files)

    return flask.render_template("torrent.html", **context)


@app.route("/statistics")
@requires_auth
def statistics():
    s = Search(using=magneticod_db)
    s.aggs.bucket("stats", {
        "filter": {'range': {'found_at': {'gte': 'now-30d', 'lte': 'now'}}},
        "aggs": {"count": {"date_histogram": {"field": "found_at",
                                              "interval": "day"}}}})
    print(s.to_dict())
    r = s.execute()

    return flask.render_template("statistics.html", **{
        # We directly substitute them in the JavaScript code.
        "dates": str([t['key'] for t in r.aggs.stats.count.buckets]),
        "amounts": str([t['doc_count'] for t in r.aggs.stats.count.buckets])
    })


@app.route("/feed")
def feed():



    filter_ = flask.request.args["filter"]
    # Check for all possible users who might be requesting.
    # pylint disabled: because we do monkey-patch! [in magneticow.__main__.py:main()]
    if not app.arguments.noauth:
        hash_ = flask.request.args["hash"]
        for username, password in app.arguments.user:  # pylint: disable=maybe-no-member
            if generate_feed_hash(username, password, filter_) == hash_:
                break
        else:
            return flask.Response(
                "Could not verify your access level for that URL (wrong hash).\n",
                401
            )

    context = {}

    if filter_:
        context["title"] = "`%s` - magneticow" % (filter_,)
        with magneticod_db:
            cur = magneticod_db.execute(
                "SELECT "
                "    name, "
                "    info_hash "
                "FROM torrents "
                "INNER JOIN ("
                "    SELECT docid AS id, rank(matchinfo(fts_torrents, 'pcnxal')) AS rank "
                "    FROM fts_torrents "
                "    WHERE name MATCH ? "
                "    ORDER BY rank ASC"
                "    LIMIT 50"
                ") AS ranktable USING(id);",
                (filter_, )
            )
            context["items"] = [{"title": r[0], "info_hash": r[1].hex()} for r in cur]
    else:
        context["title"] = "The Newest Torrents - magneticow"
        with magneticod_db:
            cur = magneticod_db.execute(
                "SELECT "
                "    name, "
                "    info_hash "
                "FROM torrents "
                "ORDER BY id DESC LIMIT 50"
            )
            context["items"] = [{"title": r[0], "info_hash": r[1].hex()} for r in cur]

    return flask.render_template("feed.xml", **context), 200, {"Content-Type": "application/rss+xml; charset=utf-8"}


def initialize_magneticod_db() -> None:
    global magneticod_db

    logging.info("Connecting to magneticod's database...")

    magneticod_db = Elasticsearch('192.168.200.4')
