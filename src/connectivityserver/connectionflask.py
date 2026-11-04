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
from functools import lru_cache
from io import StringIO
import socket
import time
from threading import RLock
from urllib.parse import urlparse

from flask import Flask, abort, jsonify, make_response, request
from daqpytools.logging import get_daq_logger
import logging 
# Some functions exit with an abort(NNN) instead of return so don't complain!
# ruff: noqa RET503

partitions = {}
partlock = RLock()

# See README for logging explanations and use of CONNECTION_FLASK_DEBUG
debug_level = int(os.environ["CONNECTION_FLASK_DEBUG"]) if "CONNECTION_FLASK_DEBUG" in os.environ else 1
log_level_mapping = {
    0 : logging.WARNING,
    1 : logging.INFO,
    2 : logging.DEBUG,
}
log = get_daq_logger(__name__, log_level=log_level_mapping.get(debug_level, logging.INFO), stream_handlers=True)

def map_uri_to_scheme(uri: str) -> str:
    """
    Extract a scheme from a URI.

    Args:
        uri (str): The URI string to be parsed.

    Returns:
        str: The scheme, or an empty string if absent or invalid.
    """

    if not isinstance(uri, str):
        log.warning("Could not parse URI scheme: expected a string, got %s", type(uri).__name__)
        return ""

    try:
        return urlparse(uri).scheme
    except ValueError as e:
        log.warning("Could not parse scheme for URI %r: %s", uri, e)
        return ""


def map_uri_to_port(uri: str):
    """
    Extract a port from a URI.

    Args:
        uri (str): The URI string to be parsed.

    Returns:
        int, str or None: The port, None if absent, or an empty string if invalid.
    """

    if not isinstance(uri, str):
        log.warning("Could not parse URI port: expected a string, got %s", type(uri).__name__)
        return ""

    try:
        return urlparse(uri).port
    except ValueError as e:
        log.warning("Could not parse port for URI %r: %s", uri, e)
        return ""


@lru_cache(maxsize=4096)
def map_uri_to_hostname(uri: str) -> str:
    """
    Map a URI to a hostname.

    Args:
        uri (str): The URI string to be mapped.

    Returns:
        str: The resolved hostname or an empty string if resolution fails.
    """

    try:
        ip_address = urlparse(uri).hostname
        if not ip_address:
            raise ValueError(f"Invalid URI format: {uri}. No hostname found.")

    except ValueError as e:
        log.debug(f"Could not parse {uri}, using the local outbound IP")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(0.1)
                s.connect(("10.255.255.255", 1))
                ip_address = s.getsockname()[0]
        except OSError as e:
            log.warning(f"Could not determine the local outbound IP for URI {uri}: {e}")
            return ""

    try:
        hostname, _, _ = socket.gethostbyaddr(ip_address)
        return hostname
    except (socket.herror, socket.gaierror, OSError) as e:
        log.debug(f"Could not resolve host {ip_address}: {e}")
        return ""


def map_uri_to_resolved_uri(uri: str) -> str:
    """
    Resolve the hostname and replace it in the URI.

    Args:
        uri (str): The URI string to be resolved.

    Returns:
        str: The resolved URI with the hostname replaced, or the original URI if resolution fails.
    """

    # Parse the URI and extract the hostname and port.
    try:
        parsed = urlparse(uri)
        if not parsed.hostname:
            log.warning(f"Could not resolve URI {uri}: no hostname found")
            return uri
        port = parsed.port
    except ValueError as e:
        log.warning(f"Could not resolve URI {uri}: {e}")
        return uri

    # Get the resolved hostname for the URI.
    hostname = map_uri_to_hostname(uri)
    if not hostname:
        return uri

    # Construct the netloc with the resolved hostname and the original port if present.
    netloc = hostname
    if port is not None:
        netloc += f":{port}"

    # Replace the netloc in the parsed URI with the constructed netloc.
    return parsed._replace(netloc=netloc).geturl()


def invalid_request(message: str):
    """
    If the request is invalid, log a warning and abort with a 400 status code.

    Args:
        message: The message describing why the request is invalid.

    Returns:
        None
    """
    log.warning("Invalid request to %s: %s", request.path, message)
    abort(400, description=message)


def read_request_json():
    """
    Validate and parse the JSON body of the request.

    Returns:
        dict: The parsed JSON object.
    
    Returns:
        None: If the JSON is invalid or not a dictionary, the request is aborted.
    """
    try:
        js = json.loads(request.data)
    except (ValueError, UnicodeDecodeError) as e:
        invalid_request(f"Invalid JSON: {e}")
    if not isinstance(js, dict):
        invalid_request("Expected a JSON object")
    return js



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
    """
    Health check endpoint to verify if the server is alive.

    Args:
        None

    Returns:
        tuple: A tuple containing the response message and the HTTP status code.
    """
    return "OK", 200


@app.route("/ready", methods=["GET"])
def ready():
    """
    Server readiness check endpoint to verify if the server is ready to handle requests.

    Args:
        None

    Returns:
        tuple: A tuple containing the response message and the HTTP status code.
    """
    if appstarted:
        return "OK", 200
    return "Not Ready", 503


@app.route("/")
def dump():
    """
    Dump the current configuration and active partitions of the server.

    This endpoint generates an HTML representation of the server's current state,
    including active partitions and their entries.

    Args:
        None

    Returns:
        str: HTML string representing the server's configuration and active partitions.
    """
    # Get the current time to check for expired entries.
    now = datetime.now()

    # Construct the start of the HTML dump stream.
    dstream = StringIO()
    dstream.write("<h1>Dump of configuration dictionary</h1>")
    dstream.write("<h2>Active partitions</h2><p>")

    # Make a copy of the current partitions so the lock is freed immediately.
    with partlock:
        partition_copy = {p: store.copy() for p, store in partitions.items()}

    # If there are partitions, generate the HTML table for them.
    if partition_copy:
        pad = ' style="padding-left: 1em;padding-right: 1em"'
        dstream.write(
            f'<table style="border: 1px solid black">'
            f'<tr style="background: #e0e0e0"><th{pad}>Partition</th>'
            f"<th{pad}>Entries</th></tr>"
        )

        # Generate the HTML rows for each partition with its entry count.
        for p in partition_copy:
            dstream.write(
                f"<tr><td{pad}>{p}</td><td{pad}>{len(partition_copy[p])}</td></tr>"
            )

        # Set the table for active partitions.
        dstream.write("</table>")
        dstream.write(f"<h2>Partitions</h2>")
        for p in partition_copy:
            store = partition_copy[p]
            dstream.write(f"<h3>{p}</h3>")
            dstream.write(
                f'<table style="border: 1px solid black">'
                f'<tr style="background: #e0e0e0">'
                f'<th{pad} rowspan="2">Name</th>'
                f'<th{pad} colspan="6">Connection</th>'
                f"</tr>"
                f'<tr style="background: #e0e0e0">'
                f"<th{pad}>uri</th>"
                f"<th{pad}>uri (resolved)</th>"
                f"<th{pad}>data_type</th>"
                f"<th{pad}>capacity</th>"
                f"<th{pad}>connection_type</th>"
                f"<th{pad}>time</th>"
                f"</tr>"
            )
            # Red strike through expired entries.
            format_cell = lambda value, strike: (
                f'<span style="color: red;">{value}</span>' if strike else f"{value}"
            )

            # Iterate over each entry in the partition and generate its HTML row.
            for k, v in store.items():
                expired = now - v.time >= entry_ttl
                resolved_hostname = map_uri_to_resolved_uri(v.uri)

                dstream.write(
                    f"<tr><td{pad}>{format_cell(k, expired)}</td>"
                    f"<td{pad}>{format_cell(v.uri, expired)}</td>"
                    f"<td{pad}>{format_cell(resolved_hostname, expired)}</td>" # Render resolved uri row
                    f"<td{pad}>{format_cell(v.data_type, expired)}</td>"
                    f"<td{pad}>{format_cell(v.capacity, expired)}</td>"
                    f"<td{pad}>{format_cell(v.connection_type, expired)}</td>"
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
    stats_to_html(dstream)

    # Return the generated HTML content.
    dstream.seek(0)
    return dstream.read()


def stats_to_html(dstream) -> None:
    """
    Render the server statistics into the provided HTML stream.

    This includes information about publish and lookup calls, their timings,
    and the maximum number of partitions and entries observed.

    Args:
        dstream (StringIO): The HTML stream to write the statistics to.

    Returns:
        None: This function writes the statistics to the provided HTML stream.

    Raises:
        None.
    """
    with partlock:
        # Write the timestamp of the last statistics update.
        dstream.write(f"<p>Since {last_stats}</p>")

        # Report the average publish time per call.
        if npublishes > 0:
            avg_pub = publish_time / npublishes
        else:
            avg_pub = timedelta()
        dstream.write(
            f"<p>{npublishes} calls to publish in total time {publish_time} "
            f"(average {avg_pub.total_seconds() * 1000000:g} &micro;s per call)</p>"
        )

        # Report the average lookup time per call.
        if nlookups > 0:
            avg_lookup = lookup_time / nlookups
        else:
            avg_lookup = timedelta()
        dstream.write(
            f"<p>{nlookups} calls to lookup in total time {lookup_time} "
            f"(average {avg_lookup.total_seconds() * 1000000:g} &micro;s per call)</p>"
        )

        # Display the maximum number of partitions and entries observed.
        dstream.write(f"<p>Maximum number of partitions active = {maxpartitions}</p>")
        for part in maxentries:
            dstream.write(
                f"<p>Maximum entries in partition {part} = {maxentries[part]}</p>"
            )


@app.route("/stats")
def dumpStats():
    """
    Dump the current server statistics as an HTML string.

    Args:
        None.

    Returns:
        str: The HTML content representing the server statistics.

    Raises:
        None.
    """
    # Create a StringIO stream to hold the HTML content.
    dstream = StringIO()

    # Write the statistics section.
    dstream.write("<h1>Connection server statistics</h1>")
    stats_to_html(dstream)

    # Return the HTML content as a string.
    dstream.seek(0)
    return dstream.read()


@app.route("/resetStats")
def resetStats():
    """
    Reset the server statistics and return the previous statistics as an HTML string.

    Args:
        None.

    Returns:
        str: The HTML content representing the previous server statistics.

    Raises:
        None.
    """
    global \
        last_stats, \
        npublishes, \
        nlookups, \
        lookup_time, \
        publish_time, \
        maxpartitions, \
        maxentries

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
def reset():
    """
    Reset the service by clearing all partitions and resetting the server statistics.

    Args:
        None.

    Returns:
        str: The HTML content representing the previous server statistics.

    Raises:
        None.
    """

    global partitions
    with partlock:
        partitions = {}
        return resetStats()


@app.route("/publish", methods=["POST"])
def publish():
    """
    Register connection information for a given partition.
    Expects a JSON payload with the following structure:
    {
        "partition": "<partition_name>",
        "connections": [
            {
                "uid": "<unique_id>",
                "uri": "<connection_uri>",
                "connection_type": "<type>",
                "data_type": "<data_type>",
                "capacity": <capacity>  # optional
            },
            ...
        ]
    }

    Invalid payloads are logged and rejected with HTTP 400 before any updates.

    Args:
        None.

    Returns:
        str: "OK" if the connections were successfully published.

    Raises:
        ValueError: If the request payload is invalid.
    """
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
        if any(not isinstance(connection.get(key), str) for key in ("uid", "uri", "data_type")):
            invalid_request("Each connection requires string uid, uri and data_type")
        if type(connection.get("connection_type")) is not int:
            invalid_request("connection_type must be an integer")
        if "capacity" in connection and type(connection["capacity"]) is not int:
            invalid_request("capacity must be an integer")

    # Log the incoming request for debugging
    log.debug(f"{js=}")
    part = js["partition"]

    log.debug(
        f"{len(js['connections'])} connections in partition {part} from {request.remote_addr} uri={js['connections'][0]['uri']}..."
    )

    # Resolve connection details outside the lock because DNS resolution can be slow.
    connection_details = []
    for connection in js["connections"]:
        _port = None
        _hostname = ""
        if "uid" in connection and "uri" in connection:
            _uri = connection["uri"]
            _port = map_uri_to_port(_uri)
            _hostname = map_uri_to_hostname(_uri)
        connection_details.append((connection, _port, _hostname))

    with partlock:
        # Initialize or retrieve the store for the current partition.
        timestamp = datetime.now()
        if part in partitions:
            store = partitions[part]
        else:
            store = {}
            partitions[part] = store

        # Update global statistics for partitions and entries.
        global maxpartitions
        if len(partitions) > maxpartitions:
            maxpartitions = len(partitions)
        if part not in maxentries:
            maxentries[part] = 0

        # Keep track of the applications already registered in this partition.
        registered_partition_applications = list(store.keys())

        # Define a named tuple to store connection details in a structured manner.
        Connection = namedtuple(
            "Connection", ["uri", "data_type", "capacity", "connection_type", "time"]
        )

        # Iterate over each connection and update the store accordingly.
        for connection, _port, _hostname in connection_details:
            if "uid" in connection and "uri" in connection:
                uid = connection["uid"]
                now = datetime.now()

                # Log whether we are updating an existing application or registering a new one.
                if uid in registered_partition_applications:
                    log.debug(f"Updating address of existing application {uid} with uri {connection['uri']}")
                else:
                    _uri = connection["uri"]
                    log.info(f"Registering new application {uid} with uri {_uri} ({_hostname}:{_port})")

                # Store the connection details in the partition's store.
                store[uid] = Connection(
                    uri=connection["uri"],
                    connection_type=connection["connection_type"],
                    data_type=connection["data_type"],
                    capacity=(
                        connection["capacity"] if "capacity" in connection else 0
                    ),
                    time=timestamp,
                )

        # Log the completion of processing all connections for this partition.
        elapsed = timedelta(seconds=time.monotonic() - time_start)
        log.debug(
            f"Took {elapsed.total_seconds() * 1000000:g} us to add {len(js['connections'])} connections"
        )

        # Update global publish statistics.
        global npublishes, publish_time
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
        404: If the partition is not found.
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
        str: "OK" if all specified connections were successfully retracted.

    Raises:
        404: If any connection is not found.
    """
    # Read and validate the request JSON.
    js = read_request_json()
    if not isinstance(js.get("partition"), str):
        invalid_request("partition must be a string")
    if not isinstance(js.get("connections"), list):
        invalid_request("connections must be a list")
    for connection in js["connections"]:
        if not isinstance(connection, dict) or not isinstance(connection.get("connection_id"), str):
            invalid_request("Each retraction requires a string connection_id")

    # Initialize a flag to track that specified connections were successfully retracted.
    good = True
    part = js["partition"]
    with partlock:
        # Check if the partition exists before attempting to retract connections.
        if part not in partitions:
            return make_response(f"Partition {part} not found", 404)

        # Attempt to retract each specified connection from the partition.
        store = partitions[part]
        for con in js["connections"]:
            id = con["connection_id"]
            if id in store:
                store.pop(id)
            else:
                log.info(f"Could not find connection_id <{id}>")
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
    Retrieve connections from a partition that match the specified UID regex.

    Args:
        part (str): The partition from which to retrieve connections.

    Returns:
        list: A list of connections matching the specified criteria.

    Raises:
        404: If the partition is not found.
    """
    now = datetime.now()
    js = read_request_json()
    log.debug(f"{js=}")

    # Validate the request JSON for required fields.
    if isinstance(js.get("uid_regex"), str) and isinstance(js.get("data_type"), str):
        log.debug(
            f"Searching for connections matching uid_regex<{js['uid_regex']}> and data_type {js['data_type']}"
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
            result.append({
                "uid": uid,
                "uri": con.uri,
                "connection_type": con.connection_type,
                "data_type": con.data_type,
                "capacity": con.capacity,
            })

        # Log the time required to perform the lookup.
        td = datetime.now() - now
        log.debug(
            f"Lookup took {td.microseconds} us to find {len(result)} connections"
        )
        global nlookups, lookup_time
        with partlock:
            nlookups += 1
            lookup_time += td

        return jsonify(result)

    else:
        invalid_request("uid_regex and data_type must be strings")
