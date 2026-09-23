#
# @file connection-flask.py Simple prototype connection configuration server
# This is part of the DUNE DAQ software, copyright 2020.
#  Licensing/copyright details are in the COPYING file that you should have
#  received with this code.
#

import json
import os
import re
from collections import namedtuple
from datetime import datetime, timedelta
from io import StringIO
import socket
from threading import Lock
from urllib.parse import urlparse

from flask import Flask, abort, make_response, request
from daqpytools.logging import get_daq_logger

# Some functions exit with an abort(NNN) instead of return so don't complain!
# ruff: noqa RET503

partitions = {}
partlock = Lock()

if "CONNECTION_FLASK_DEBUG" in os.environ:
    debug_level = int(os.environ["CONNECTION_FLASK_DEBUG"])
else:
    debug_level = 1


def convert_log_level(log_level):
    if log_level == 0:
        return 30# logging.WARNING
    if log_level == 1:
        return 20# logging.INFO
    if log_level == 2:
        return 10# logging.DEBUG
    return 20# logging.INFO


def map_uri_to_hostname(uri: str) -> str:
    """
    Map a URI to a hostname. This function is used to extract the hostname from a URI string.

    Args:
        uri (str): The URI string to be mapped.

    Returns:
        str: The extracted hostname from the URI.

    Raises:
        ValueError: If the URI is not in a valid format or does not contain a hostname.
    """

    try:
       ip_address = urlparse(uri).hostname
        if not ip_address:
            raise ValueError(f"Invalid URI format: {uri}. No hostname found.")

        hostname, aliases, ip_list = socket.gethostbyaddr(ip_address)
        return hostname

    except socket.herror:
        return f"No reverse DNS record found for IP: {ip_address}"
    except Exception as e:
        return f"Error parsing URI or resolving host: {e}"





log = get_daq_logger(__name__, log_level=convert_log_level(debug_level), stream_handlers=True)

if "ENTRY_TTL" in os.environ:
    ttl = int(os.environ["ENTRY_TTL"])
else:
    ttl = 10
entry_ttl = timedelta(seconds=ttl)

last_stats = datetime.now()
npublishes = 0
nlookups = 0
lookup_time = timedelta(0)
publish_time = timedelta()
maxpartitions = 0
maxentries = {}

app = Flask(__name__)
global appstarted
appstarted = False


with app.app_context():
    appstarted = True


@app.route("/live", methods=["GET"])
def live():
    return "OK", 200


@app.route("/ready", methods=["GET"])
def ready():
    if appstarted:
        return "OK", 200
    return "Not Ready", 503


@app.route("/")
def dump():
    now = datetime.now()
    dstream = StringIO()
    dstream.write("<h1>Dump of configuration dictionary</h1>")
    dstream.write("<h2>Active partitions</h2><p>")
    if len(partitions) > 0:
        pad = ' style="padding-left: 1em;padding-right: 1em"'
        dstream.write(
            f'<table style="border: 1px solid black">'
            f'<tr style="background: #e0e0e0"><th{pad}>Partition</th>'
            f"<th{pad}>Entries</th></tr>"
        )
        for p in partitions:
            dstream.write(
                f"<tr><td{pad}>{p}</td><td{pad}>{len(partitions[p])}</td></tr>"
            )
        dstream.write("</table>")
        dstream.write(f"<h2>Partitions</h2>")
        for p in partitions:
            store = partitions[p]
            dstream.write(f"<h3>{p}</h3>")
            dstream.write(
                f'<table style="border: 1px solid black">'
                f'<tr style="background: #e0e0e0">'
                f'<th{pad} rowspan="2">Name</th>'
                f'<th{pad} colspan="6">Connection</th>'  # Increased colspan from 5 to 6
                f"</tr>"
                f'<tr style="background: #e0e0e0">'
                f"<th{pad}>uri</th>"
                f"<th{pad}>uri (resolved)</th>"         # Added the new header entry
                f"<th{pad}>data_type</th>"
                f"<th{pad}>capacity</th>"
                f"<th{pad}>connection_type</th>"
                f"<th{pad}>time</th>"
                f"</tr>"
            )
            format_cell = lambda value, strike: (
                f'<span style="color: red;">{value}</span>' if strike else f"{value}"
            )
            for k, v in store.items():
                expired = now - v.time >= entry_ttl
                resolved_hostname = urlparse(v.uri).scheme + "://" + map_uri_to_hostname(v.uri) # Call resolution function
                
                dstream.write(
                    f"<tr><td{pad}>{format_cell(k, expired)}</td>"
                    f"<td{pad}>{format_cell(v.uri, expired)}</td>"
                    f"<td{pad}>{format_cell(resolved_hostname, expired)}</td>" # Render resolved uri row
                    f"<td{pad}>{format_cell(v.data_type, expired)}</td>"
                    f"<td{pad}>{format_cell(v.capacity, expired)}</td>"
                    f"<td{pad}>{format_cell(v.connection_type, expired)}</td>"
                    f"<td{pad}>{format_cell(v.time, expired)}</td></tr>"
                )
            dstream.write("</table>")
    else:
        dstream.write("None</p>")
    dstream.write("<hr><h2>Server statistics</h2>")
    stats_to_html(dstream)
    dstream.seek(0)
    return dstream.read()


def stats_to_html(dstream):
    dstream.write(f"<p>Since {last_stats}</p>")
    if npublishes > 0:
        avg_pub = publish_time / npublishes
    else:
        avg_pub = timedelta()
    dstream.write(
        f"<p>{npublishes} calls to publish in total time {publish_time} "
        f"(average {avg_pub.microseconds} &micro;s per call)</p>"
    )
    if nlookups > 0:
        avg_lookup = lookup_time / nlookups
    else:
        avg_lookup = timedelta()
    dstream.write(
        f"<p>{nlookups} calls to lookup in total time {lookup_time} "
        f"(average {avg_lookup.microseconds} &micro;s per call)</p>"
    )
    dstream.write(f"<p>Maximum number of partitions active = {maxpartitions}</p>")
    for part in maxentries:
        dstream.write(
            f"<p>Maximum entries in partition {part} = {maxentries[part]}</p>"
        )


@app.route("/stats")
def dumpStats():
    dstream = StringIO()
    dstream.write("<h1>Connection server statistics</h1>")
    stats_to_html(dstream)
    dstream.seek(0)
    return dstream.read()


@app.route("/resetStats")
def resetStats():
    stats = dumpStats()

    global \
        last_stats, \
        npublishes, \
        nlookups, \
        lookup_time, \
        publish_time, \
        maxpartitions, \
        maxentries

    last_stats = datetime.now()
    npublishes = 0
    nlookups = 0
    lookup_time = timedelta(0)
    publish_time = timedelta()
    maxpartitions = 0
    maxentries = {}

    return stats


@app.route("/resetService")
def reset():

    global partitions
    partitions = {}
    return resetStats()


@app.route("/publish", methods=["POST"])
def publish():
    #  Store multiple connection ids and corresponding uris in a
    #  dictionary associated with the appropriate partition.
    timestamp = datetime.now()
    js = json.loads(request.data)

    log.debug(f"{js=}")
    part = js["partition"]

    log.debug(
        f"{len(js['connections'])} connections in partition {part} from {request.remote_addr} uri={js['connections'][0]['uri']}..."
    )

    with partlock:
        if part in partitions:
            store = partitions[part]
        else:
            store = {}
            partitions[part] = store
            global maxpartitions
            if len(partitions) > maxpartitions:
                maxpartitions = len(partitions)
            if part not in maxentries:
                maxentries[part] = 0

        registered_partition_applications = list(store.keys())

        Connection = namedtuple(
            "Connection", ["uri", "data_type", "capacity", "connection_type", "time"]
        )

        for connection in js["connections"]:
            if "uid" in connection and "uri" in connection:
                uid = connection["uid"]
                now = datetime.now()

                # current_keys = [str(k) for k in store.keys()]
                # keys_str = ", ".join(str(k) for k in store.keys())
                # log.warning(f"store_keys={keys_str}")
                if uid in registered_partition_applications:
                    log.debug(f"Updating address of existing application {uid} with uri {connection['uri']}")
                else:
                    _uri = connection["uri"]
                    _port = urlparse(_uri).port
                    _hostname = map_uri_to_hostname(_uri)
                    log.info(f"Registering new application {uid} with uri {_uri} ({_hostname}:{_port})")

                store[uid] = Connection(
                    uri=connection["uri"],
                    connection_type=connection["connection_type"],
                    data_type=connection["data_type"],
                    capacity=(
                        connection["capacity"] if "capacity" in connection else 0
                    ),
                    time=timestamp,
                )

            now = datetime.now()
            elapsed = now - timestamp

            log.debug(
                f"Took {elapsed.microseconds} us to add {len(js['connections'])} connections"
            )

            global npublishes, publish_time
            publish_time += elapsed
            npublishes += 1

            if len(store) > maxentries[part]:
                maxentries[part] = len(store)


    return "OK"


@app.route("/retract-partition", methods=["POST"])
def retract_partition():
    if len(request.data) == 0:
        abort(400)

    js = json.loads(request.data)
    log.warning(f"request=[{js}]")

    if "partition" not in js:
        abort(400)

    part = js["partition"]
    partlock.acquire()

    if part in partitions:
        partitions.pop(part)
        partlock.release()
        return "OK"
    partlock.release()
    abort(404)


@app.route("/retract", methods=["POST"])
def retract():
    if len(request.data) == 0:
        abort(400)

    js = json.loads(request.data)
    good = True
    part = js["partition"]
    partlock.acquire()
    if part not in partitions:
        partlock.release()
        return make_response(f"Partition {part} not found", 404)

    store = partitions[part]
    if "connections" not in js:
        abort(400)
    for con in js["connections"]:
        id = con["connection_id"]
        if id in store:
            store.pop(id)
        else:
            log.info(f"Could not find connection_id <{id}>")
            good = False
    if len(store) == 0:
        # We've deleted the last entry in this partition so delete the
        # partition as well
        partitions.pop(part)
    partlock.release()
    if good:
        return "OK"
    abort(404)


@app.route("/getconnection/<part>", methods=["POST", "GET"])
def get_connection(part):
    if len(request.data) == 0:
        abort(400)

    # Find connection uris that correspond to the connection id pattern
    # in the request. The pattern is treated as a regular expression.

    now = datetime.now()
    js = json.loads(request.data)
    log.debug(f"{js=}")

    if "uid_regex" in js and "data_type" in js:
        log.debug(
            f"Searching for connections matching uid_regex<{js['uid_regex']}> and data_type {js['data_type']}"
        )

        result = []
        regex = re.compile(js["uid_regex"])
        dt = js["data_type"]
        partlock.acquire()

        if part in partitions:
            store = partitions[part]
            matched = []
            for uid, con in store.items():
                if (
                    regex.fullmatch(uid)
                    and con.data_type == dt
                    and now - con.time < entry_ttl
                ):
                    matched.append((uid, con))
            partlock.release()
            # We should now be able to construct JSON string while other threads
            # have access to the partition dict
            for uid, con in matched:
                result.append(
                    "{"
                    f'"uid":"{uid}",'
                    f'"uri":"{con.uri}",'
                    f'"connection_type":{con.connection_type},'
                    f'"data_type":"{con.data_type}",'
                    f'"capacity":{con.capacity}'
                    "}"
                )

            td = datetime.now() - now
            log.debug(
                f"Lookup took {td.microseconds} us to find {len(result)} connections"
            )
            global nlookups, lookup_time
            # Should we have the lock while updating statistics? It doesn't
            # really matter if the stats aren't entirely accurate.
            nlookups += 1
            lookup_time += td

            return "[" + ",".join(result) + "]"

        partlock.release()
        log.debug(f"Partition {part} not found")
        abort(404)
    else:
        abort(400)
