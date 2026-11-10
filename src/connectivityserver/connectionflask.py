#
# @file connection-flask.py Simple prototype connection configuration server
# This is part of the DUNE DAQ software, copyright 2020.
#  Licensing/copyright details are in the COPYING file that you should have
#  received with this code.
#

import json
import os
import re
import time
from datetime import datetime, timedelta
from html import escape
from io import StringIO
from threading import RLock

from flask import Flask, abort, jsonify, make_response, request

from connectivityserver.utils import (
    Connection,
    format_cell,
    format_connection_type,
    log,
    map_uri_to_hostname,
    map_uri_to_port,
    map_uri_to_resolved_uri,
    stats_to_html,
)

partitions: dict[str, dict[str, Connection]] = {}
partlock = RLock()

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
appstarted = False


with app.app_context():
    appstarted = True


def invalid_request(message: str) -> None:
    """
    Log an invalid Flask request and reject it with HTTP 400.

    Args:
        message (str): The error message describing the invalid request.

    Returns:
        None

    Raises:
        werkzeug.exceptions.HTTPException: Always raises an HTTP 400 error with the 
            provided message.
    """
    log.warning("Invalid request to %s: %s", request.path, message)
    abort(400, description=message)


def read_request_json() -> dict:
    """
    Parse the current Flask request and require a JSON object.

    Args:
        None

    Returns:
        dict: The parsed JSON object from the request.

    Raises:
        werkzeug.exceptions.HTTPException: If the request does not contain valid JSON or
            is not a JSON object.
    """
    try:
        js = json.loads(request.data)
    except (ValueError, UnicodeDecodeError) as error:
        invalid_request(f"Invalid JSON: {error}")
    if not isinstance(js, dict):
        invalid_request("Expected a JSON object")
    return js


@app.route("/live", methods=["GET"])
def live():
    """
    Health check endpoint to verify if the server is alive.

    Returns:
        tuple: A tuple containing the response message and the HTTP status code.
    """
    return "OK", 200


@app.route("/ready", methods=["GET"])
def ready():
    """
    Server readiness check endpoint to verify if the server is ready to handle requests.

    Returns:
        tuple: A tuple containing the response message and the HTTP status code.
    """
    if appstarted:
        return "OK", 200
    return "Not Ready", 503


@app.route("/")
def dump() -> str:
    """
    Dump the current configuration and active partitions of the server.

    Render a locked snapshot of stored partitions, including expired entries
    highlighted in red.

    Args:
        None

    Returns:
        str: HTML string representing the server's configuration and active partitions.

    Raises:
        None
    """
    # Get the current time to check for expired entries.
    now = datetime.now()

    # Construct the start of the HTML dump stream.
    dstream = StringIO()
    dstream.write("<h1>Connectivity Server</h1>")
    dstream.write("<h2>Active session summary</h2><p>")

    # Snapshot before rendering so DNS does not hold the partition lock.
    with partlock:
        partitions_copy = {p: store.copy() for p, store in partitions.items()}

    # If there are partitions, generate the HTML table for them.
    if partitions_copy:
        pad = ' style="padding-left: 1em;padding-right: 1em"'
        dstream.write(
            f'<table style="border: 1px solid black">'
            f'<tr style="background: #e0e0e0"><th{pad}>Session</th>'
            f"<th{pad}>Connections</th></tr>"
        )

        # Generate the HTML rows for each partition with its entry count.
        for p in partitions_copy:
            length = len(partitions_copy[p])
            dstream.write(
                f"<tr><td{pad}>{escape(p)}</td><td{pad}>{length}</td></tr>"
            )

        # Set the table for active partitions.
        dstream.write("</table>")
        dstream.write("<h2>Sessions</h2>")
        for p in partitions_copy:
            store = partitions_copy[p]
            dstream.write(f"<h3>{escape(p)}</h3>")
            dstream.write(
                f'<table style="border: 1px solid black">'
                f'<tr style="background: #e0e0e0">'
                f'<th{pad} rowspan="2">Application</th>'
                f'<th{pad} colspan="6">Connection</th>'
                f"</tr>"
                f'<tr style="background: #e0e0e0">'
                f"<th{pad}>URI</th>"
                f"<th{pad}>Resolved URI</th>"
                f"<th{pad}>Endpoint type</th>"
                f"<th{pad}>Capacity</th>"
                f"<th{pad}>Connection type (ZMQ mapped)</th>"
                f"<th{pad}>Last updated time</th>"
                f"</tr>"
            )
            # Iterate over each entry in the partition and generate its HTML row.
            for k, v in store.items():
                expired = now - v.time >= entry_ttl
                resolved_uri = map_uri_to_resolved_uri(v.uri)
                formatted_connection_type = format_connection_type(v.connection_type)

                dstream.write(
                    f"<tr><td{pad}>{format_cell(k, expired)}</td>"
                    f"<td{pad}>{format_cell(v.uri, expired)}</td>"
                    f"<td{pad}>{format_cell(resolved_uri, expired)}</td>"
                    f"<td{pad}>{format_cell(v.data_type, expired)}</td>"
                    f"<td{pad}>{format_cell(v.capacity, expired)}</td>"
                    f"<td{pad}>{format_cell(formatted_connection_type, expired)}</td>"
                    f"<td{pad}>{format_cell(v.time, expired)}</td></tr>"
                )

            # Close the HTML table for the current partition.
            dstream.write("</table>")
    else:
        # If there are no active partitions, indicate that in the HTML.
        dstream.write("None</p>")

    # Add a horizontal rule and server statistics section.
    dstream.write("<hr><h2>Server statistics</h2>")

    # Render the server statistics into the HTML stream.
    with partlock:
        stats_to_html(
            dstream,
            last_stats,
            npublishes,
            publish_time,
            nlookups,
            lookup_time,
            maxpartitions,
            maxentries,
        )

    # Return the generated HTML content.
    dstream.seek(0)
    return dstream.read()


@app.route("/stats")
def dumpStats() -> str:
    """
    Dump the current server statistics as an HTML string.

    Args:
        None

    Returns:
        str: The HTML content representing the server statistics.

    Raises:
        werkzeug.exceptions.HTTPException: If the request does not contain valid JSON or
            is not a JSON object.
    """
    # Create a StringIO stream to hold the HTML content.
    dstream = StringIO()

    # Write the statistics section.
    dstream.write("<h1>Connection server statistics</h1>")
    with partlock:
        stats_to_html(
            dstream,
            last_stats,
            npublishes,
            publish_time,
            nlookups,
            lookup_time,
            maxpartitions,
            maxentries,
        )

    # Return the HTML content as a string.
    dstream.seek(0)
    return dstream.read()


@app.route("/resetStats")
def resetStats() -> str:
    """
    Reset the server statistics and return the previous statistics as an HTML string.

    Published connections and the hostname cache are preserved.

    Args:
        None

    Returns:
        str: The HTML content representing the previous server statistics.

    Raises:
        werkzeug.exceptions.HTTPException: If the request does not contain valid JSON or
            is not a JSON object.

    """
    global last_stats, npublishes, nlookups, lookup_time, publish_time
    global maxpartitions, maxentries

    with partlock:
        stats = dumpStats()
        last_stats = datetime.now()
        npublishes = 0
        nlookups = 0
        lookup_time = timedelta(0)
        publish_time = timedelta()
        maxpartitions = 0
        maxentries = {}

    return stats


@app.route("/resetService")
def reset() -> str:
    """
    Reset the service by clearing all partitions and resetting the server statistics.

    Connections and statistics are cleared atomically. The hostname cache is
    preserved.

    Args:
        None

    Returns:
        str: The HTML content representing the previous server statistics.

    Raises:
        werkzeug.exceptions.HTTPException: If the request does not contain valid JSON or
            is not a JSON object.
    """
    global partitions
    with partlock:
        partitions = {}
        return resetStats()


@app.route("/publish", methods=["POST"])
def publish() -> str:
    """
    Register connection information for a given partition.

    Expects a JSON payload with the following structure:
    {
        "partition": "<partition_name>",
        "connections": [
            {
                "uid": "<unique_id>",
                "uri": "<connection_uri>",
                "connection_type": 0,
                "data_type": "<data_type>",
                "capacity": 0,
                "hostname": "<optional hostname metadata>"
            },
            ...
        ]
    }

    Args:
        None

    Returns:
        str: "OK" if the connections were successfully published.

    Raises:
        werkzeug.exceptions.HTTPException: If the request does not contain valid JSON or
            is not a JSON object.
    """

    # Retrieve the global variables
    global maxpartitions, npublishes, publish_time

    # Record the start time for measuring the duration of the publish operation.
    time_start = time.monotonic()
    js = read_request_json()

    # Validate the incoming JSON payload for required fields and types.
    if not isinstance(js.get("partition"), str):
        invalid_request("partition must be a string")
    if not isinstance(js.get("connections"), list) or not js["connections"]:
        invalid_request("connections must be a non-empty list")

    # Validate each connection object within the connections list.
    for connection in js["connections"]:
        if not isinstance(connection, dict):
            invalid_request("Each connection must be an object")
        if any(
            not isinstance(connection.get(key), str)
            for key in ("uid", "uri", "data_type")
        ):
            invalid_request("Each connection requires string uid, uri and data_type")
        if type(connection.get("connection_type")) is not int:
            invalid_request("connection_type must be an integer")

        # Check the hostname format if provided
        if "hostname" in connection:
            hostname = connection["hostname"]
            if (
                not isinstance(hostname, str)
                or not hostname
                or len(hostname) > 253
                or any(
                    re.fullmatch(
                        r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label
                    )
                    is None
                    for label in (
                        hostname[:-1] if hostname.endswith(".") else hostname
                    ).split(".")
                )
            ):
                invalid_request("hostname must be a non-empty DNS hostname, not a URI")

    # Log the incoming request for debugging
    part = js["partition"]
    log.debug(f"{js=}")
    log.debug(
        f"{len(js['connections'])} connections in partition {part} from "
        f"{request.remote_addr} uri={js['connections'][0]['uri']}..."
    )

    # Resolve connection details outside the lock because DNS resolution can be slow.
    connection_details = []
    for connection in js["connections"]:
        log.debug(f"Handling connection {connection=}")
        uri = connection["uri"]
        connection_details.append(
            (connection, map_uri_to_port(uri), map_uri_to_hostname(uri))
        )

    with partlock:
        # Initialize or retrieve the store for the current partition.
        timestamp = datetime.now()
        if part in partitions:
            store = partitions[part]
        else:
            store = {}
            partitions[part] = store

        # Update global statistics for partitions and entries.
        if len(partitions) > maxpartitions:
            maxpartitions = len(partitions)
        if part not in maxentries:
            maxentries[part] = 0

        # Keep track of the applications already registered in this partition.
        registered_partition_applications = set(store)

        # Process each connection and update the store accordingly.
        for connection, port, hostname in connection_details:
            uid = connection["uid"]
            if uid in registered_partition_applications:
                log.debug(
                    f"Updating app {uid} address with uri {connection['uri']}"
                )
            else:
                log.info(
                    f"Registering endpoint {uid} with uri {connection['uri']} "
                    f"({hostname}:{port})"
                )

            # The expected capacity field is not getting populated, comparing it against
            # the expected values before displaying
            _capacity = connection.get("capacity", 0)
            if _capacity <= 0 or _capacity % 10:
                log.debug(
                    f"Malformed capacity {_capacity} for connection {connection['uri']}"
                    ", setting to 0."
                )
                _capacity = 0

            store[uid] = Connection(
                uri=connection["uri"],
                connection_type=connection["connection_type"],
                data_type=connection["data_type"],
                capacity=_capacity,
                time=timestamp,
                hostname=connection.get("hostname", ""),
            )

        # Log the completion of processing all connections for this partition.
        elapsed = timedelta(seconds=time.monotonic() - time_start)
        log.debug(
            f"Took {elapsed.total_seconds() * 1000000:g} us to add "
            f"{len(js['connections'])} connections"
        )

        # Update global publish statistics.
        publish_time += elapsed
        npublishes += 1
        if len(store) > maxentries[part]:
            maxentries[part] = len(store)

    return "OK"


@app.route("/retract-partition", methods=["POST"])
def retract_partition():
    """
    Retract an entire partition by removing it from the server's partition store.

    Returns "OK" if the partition was successfully retracted, otherwise returns a 404
    error if the partition was not found.

    Args:
        None

    Returns:
        str: "OK" if the partition was successfully retracted.

    Raises:
        werkzeug.exceptions.NotFound: If the partition to retract is not found.
    """
    js = read_request_json()
    log.debug(f"request=[{js}]")

    if not isinstance(js.get("partition"), str):
        invalid_request("partition must be a string")

    part = js["partition"]
    with partlock:
        if part in partitions:
            partitions.pop(part)
            return "OK"
    abort(404)


@app.route("/retract", methods=["POST"])
def retract():
    """
    Retract specific connections from a partition.

    Returns "OK" if all specified connections were successfully retracted, otherwise
    returns a 404 error if any connection was not found.
    
    Args:
        None

    Returns:
        str or flask.Response: "OK" on success, or an HTTP 404 response if the
            partition does not exist. An empty retraction list is accepted.

    Raises:
        werkzeug.exceptions.NotFound: If any specified connection is not found.
    """
    # Read and validate the request JSON.
    js = read_request_json()
    if not isinstance(js.get("partition"), str):
        invalid_request("partition must be a string")
    if not isinstance(js.get("connections"), list):
        invalid_request("connections must be a list")
    for connection in js["connections"]:
        if not isinstance(connection, dict) or not isinstance(
            connection.get("connection_id"), str
        ):
            invalid_request("Each retraction requires a string connection_id")

    # Initialize a flag to track that specified connections were successfully retracted.
    good = True
    part = js["partition"]
    with partlock:
        # Check if the partition exists before attempting to retract connections.
        if part not in partitions:
            log.warning(
                "Cannot retract connections: partition %r not found", part
            )
            return make_response(f"Partition {escape(part)} not found", 404)

        # Attempt to retract each specified connection from the partition.
        store = partitions[part]
        for con in js["connections"]:
            id = con["connection_id"]
            if id in store:
                store.pop(id)
            else:
                log.warning(
                    "Connection %r not found in partition %r; existing requested "
                    "connections are still retracted, but the response will be HTTP 404",
                    id,
                    part,
                )
                good = False
        if len(store) == 0:
            # Delete the partition if it has no more connections.
            partitions.pop(part)
    if good:
        return "OK"
    abort(404)


@app.route("/getconnection/<part>", methods=["POST", "GET"])
def get_connection(part):
    """
    Retrieve unexpired connections matching a UID regex and data type.

    The JSON body requires string uid_regex and data_type fields. UID matching
    uses fullmatch, not substring search. Matching occurs under the partition
    lock; JSON serialisation occurs afterwards.

    Args:
        part (str): The partition from which to retrieve connections.

    Returns:
        flask.Response: A JSON array of matching connections, including UID,
            URI, connection type, data type and capacity. May be empty.
    """
    global nlookups, lookup_time
    now = datetime.now()
    js = read_request_json()
    log.debug(f"{js=}")

    # Validate the request JSON for required fields.
    if isinstance(js.get("uid_regex"), str) and isinstance(js.get("data_type"), str):
        log.debug(
            f"Searching for connections matching uid_regex<{js['uid_regex']}> and "
            f"data_type {js['data_type']}"
        )

        # Initialize the result list to store matching connections.
        result = []
        try:
            regex = re.compile(js["uid_regex"])
        except re.error as e:
            invalid_request(f"Invalid uid_regex: {e}")

        # Extract the data type from the request JSON.
        dt = js["data_type"]
        matched = []

        # Initialize a list to store matched connections.
        with partlock:
            if part in partitions:
                store = partitions[part]
                for uid, con in store.items():
                    if (
                        regex.fullmatch(uid)
                        and con.data_type == dt
                        and now - con.time < entry_ttl
                    ):
                        matched.append((uid, con))
            else:
                log.debug(f"Partition {part} not found")
                abort(404)

        # Construct the result list from the matched connections.
        for uid, con in matched:
            result.append(
                {
                    "uid": uid,
                    "uri": con.uri,
                    "connection_type": con.connection_type,
                    "data_type": con.data_type,
                    "capacity": con.capacity,
                }
            )

        # Log the time required to perform the lookup.
        td = datetime.now() - now
        log.debug(
            f"Lookup took {td.microseconds} us to find {len(result)} connections"
        )
        with partlock:
            nlookups += 1
            lookup_time += td

        return jsonify(result)

    else:
        invalid_request("uid_regex and data_type must be strings")
