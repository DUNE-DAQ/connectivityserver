# This is part of the DUNE DAQ software, copyright 2020.
# Licensing/copyright details are in the COPYING file.

"""Non-Flask helpers for URI resolution, caching, logging and HTML rendering."""

import logging
import os
import re
import socket
import time
from collections import OrderedDict, namedtuple
from datetime import datetime, timedelta
from html import escape
from ipaddress import ip_address as parse_ip_address
from threading import RLock
from typing import TextIO
from urllib.parse import urlparse

from daqpytools.logging import get_daq_logger
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from urllib3.exceptions import HTTPError

Connection = namedtuple(
    "Connection",
    ["uri", "data_type", "capacity", "connection_type", "time", "hostname"],
)

hostname_cache = OrderedDict()
hostname_cache_lock = RLock()


debug_level = (
    int(os.environ["CONNECTION_FLASK_DEBUG"])
    if "CONNECTION_FLASK_DEBUG" in os.environ
    else 1
)


log_level_mapping = {
    0: logging.WARNING,
    1: logging.INFO,
    2: logging.DEBUG,
}


log = get_daq_logger(
    __name__,
    log_level=log_level_mapping.get(debug_level, logging.INFO),
    stream_handlers=True,
)


def map_uri_to_scheme(uri: str) -> str:
    """
    Extract a scheme from a URI.

    Args:
        uri (str): The URI string to be parsed.

    Returns:
        str: The scheme, or an empty string if absent or invalid.
    """
    if not isinstance(uri, str):
        log.warning(
            "Could not parse URI scheme: expected a string, got %s", type(uri).__name__
        )
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
        log.warning(
            "Could not parse URI port: expected a string, got %s", type(uri).__name__
        )
        return ""

    try:
        return urlparse(uri).port
    except ValueError as e:
        log.warning("Could not parse port for URI %r: %s", uri, e)
        return ""



def resolve_uri_hostname(uri: str) -> str:
    """
    Resolve through system DNS first, then optionally resolve a pod's physical host.

    IP and cluster-DNS endpoints use Kubernetes only when KUBECONFIG is set.
    A successful cluster lookup takes precedence over DNS. Cluster-DNS names
    are never returned as physical hostnames.

    Args:
        uri (str): The URI string to be resolved.

    Returns:
        str: The resolved hostname or an empty string if resolution fails.
    """
    try:
        ip_address = urlparse(uri).hostname
        if not ip_address:
            raise ValueError(f"Invalid URI format: {uri}. No hostname found.")

    except ValueError:
        log.debug(f"Could not parse {uri}")
        return ""

    hostname = resolve_hostname(ip_address)
    if re.search(r"\.(?:svc|pod)\.", hostname, re.IGNORECASE):
        hostname = ""

    try:
        address = str(parse_ip_address(ip_address))
    except ValueError:
        # Non-IP hosts use DNS; cluster names also require control-plane lookup.
        if not re.search(r"\.(?:svc|pod)\.", ip_address, re.IGNORECASE):
            return hostname
        if not os.environ.get("KUBECONFIG"):
            log.debug("Skipping cluster lookup: KUBECONFIG is not set")
            return hostname
        try:
            addresses = {
                entry[4][0]
                for entry in socket.getaddrinfo(
                    ip_address,
                    None,
                    type=socket.SOCK_STREAM,
                )
            }
        except OSError as error:
            log.debug("Cluster endpoint DNS failed for %s: %s", ip_address, error)
            return hostname
        if len(addresses) != 1:
            log.debug("Cluster endpoint %s does not identify one IP", ip_address)
            return hostname
        address = addresses.pop()

    return resolve_pod_hostname(address) or hostname


def resolve_pod_hostname(pod_ip: str) -> str:
    """
    Resolve a pod IP to its node's Hostname using explicit kubeconfig or in-cluster config.

    Attempts pod discovery via:
    1. Explicit KUBECONFIG environment variable (kubeconfig file path)
    2. In-cluster service account credentials (automatic when deployed in K8s)
    3. Returns empty string if neither is available

    Args:
        pod_ip (str): The IP address of the pod to resolve.

    Returns:
        str: The physical hostname, or an empty string when pod discovery is unavailable,
            no pod matches or discovery fails. Skips and failures are debug-logged.
    """
    kubeconfig = os.environ.get("KUBECONFIG")

    try:
        # Parse the pod IP address and prepare the Kubernetes client configuration.
        address = str(parse_ip_address(pod_ip))

        # Load the Kubernetes client configuration and set up the API client.
        configuration = client.Configuration()
        
        if kubeconfig:
            # Explicit kubeconfig file provided
            config.load_kube_config(
                config_file=kubeconfig,
                client_configuration=configuration,
                persist_config=False,
            )
            log.debug("Loaded kubeconfig from KUBECONFIG env var for pod discovery")
        else:
            # Try in-cluster configuration (automatic when running in Kubernetes)
            try:
                config.load_incluster_config(client_configuration=configuration)
                log.debug("Loaded in-cluster configuration for pod discovery")
            except ConfigException:
                log.debug("No KUBECONFIG and not running in-cluster: skipping pod discovery")
                return ""
        api_host = urlparse(configuration.host).hostname

        # Update the no_proxy configuration to include the API host.
        configuration.no_proxy = ",".join(
            filter(None, [configuration.no_proxy, api_host])
        )

        # Use the API client to query the Kubernetes API for pods matching the given IP.
        with client.ApiClient(configuration) as api_client:
            api = client.CoreV1Api(api_client)
            pods = api.list_pod_for_all_namespaces(
                field_selector=f"status.podIP={address}",
                _request_timeout=(3, 3),
            ).items
            if not pods:
                log.debug("No pod found for %s", pod_ip)
                return ""
            nodes = {pod.spec.node_name for pod in pods}
            if None in nodes or "" in nodes or len(nodes) != 1:
                raise ValueError("Pod IP does not uniquely identify the scheduled node")
            node = api.read_node(nodes.pop(), _request_timeout=(3, 3))
            names = {
                entry.address
                for entry in (node.status.addresses or [])
                if entry.type == "Hostname"
            }

        # Ensure the node reports unique Hostname and validate it.
        if len(names) != 1:
            raise ValueError("node does not report exactly one Hostname")

        # Check that the hostname is valid and conforms to the expected pattern.
        hostname = names.pop()
        if not hostname or re.fullmatch(r"[A-Za-z0-9.-]+", hostname) is None:
            raise ValueError("invalid node hostname")

        # Prefer a DNS-qualified name, retaining the reported hostname on failure.
        return resolve_hostname(hostname) or hostname
    except client.ApiException as error:
        log.debug(
            "Cluster lookup for %s failed (HTTP %s)",
            pod_ip,
            error.status,
        )
    except (OSError, HTTPError, ConfigException, ValueError) as error:
        log.debug("Cluster lookup for %s failed: %s", pod_ip, error)

    return ""


def resolve_hostname(ip_address: str) -> str:
    """
    Resolve a host or IP address, preferring a fully qualified name.

    Args:
        ip_address (str): The hostname or IP address to resolve.

    Returns:
        str: The resolved hostname or an empty string if resolution fails.

    """

    # Parse the IP address from the hostname. If parsing fails, set address to None.
    try:
        address = parse_ip_address(ip_address)
    except ValueError:
        address = None

    # Attempt to resolve the hostname to a fully qualified domain name.
    try:
        # If an IP address was successfully parsed, attempt a reverse lookup first.
        if address is not None:
            hostname, aliases, _ = socket.gethostbyaddr(str(address))
        else:
            addresses = socket.getaddrinfo(
                ip_address,
                None,  # No port or service is requested
                family=socket.AF_UNSPEC,  # Allow both IPv4 and IPv6 addresses
                type=socket.SOCK_STREAM,  # Use stream sockets (TCP)
                flags=socket.AI_CANONNAME,  # Request the canonical name of the host
            )

            # Iterate through the resolved addresses to find a FQDN.
            for _, _, _, canonical, _ in addresses:
                if "." in canonical.rstrip("."):
                    return canonical.rstrip(".")

            # If no FQDN was found, attempt reverse lookup on each resolved address.
            for _, _, _, _, sockaddr in addresses:
                try:
                    hostname, aliases, _ = socket.gethostbyaddr(sockaddr[0])
                    for candidate in [hostname] + aliases:
                        if "." in candidate.rstrip("."):
                            return candidate.rstrip(".")
                except OSError:
                    continue
            log.info("Could not obtain an FQDN for host %s", ip_address)
            return ""

        # If an IP address was successfully parsed and reverse lookup succeeded, check
        # the returned hostname and aliases for a FQDN.
        for candidate in [hostname] + aliases:
            if "." in candidate.rstrip("."):
                return candidate.rstrip(".")

        # If no FQDN was found in the hostname and aliases, return the hostname as-is.
        return hostname.rstrip(".")
    except OSError as e:
        log.info("Could not resolve host %s: %s", ip_address, e)
        return ""


def map_uri_to_hostname(uri: str) -> str:
    """
    Map a URI to a hostname, preferring a fully qualified name.

    Args:
        uri (str): The URI string to be mapped.

    Returns:
        str: The resolved hostname or an empty string if resolution fails.
    """
    # Query the hostname cache first to see if the URI has been resolved recently.
    with hostname_cache_lock:
        entry = hostname_cache.get(uri)
        if entry is not None:
            hostname, expires = entry
            if time.monotonic() < expires:
                hostname_cache.move_to_end(uri)
                return hostname
            del hostname_cache[uri]

    # Add the resolved hostname to the cache with an expiration time of 1 hour.
    hostname = resolve_uri_hostname(uri)
    with hostname_cache_lock:
        hostname_cache[uri] = (hostname, time.monotonic() + 3600)
        hostname_cache.move_to_end(uri)
        while len(hostname_cache) > 4096:
            hostname_cache.popitem(last=False)
    return hostname


def map_uri_to_resolved_uri(uri: str) -> str:
    """
    Resolve the hostname and replace it in the URI.

    Preserve the scheme, port, path, query and fragment. User information in
    the authority is not supported and is dropped on successful resolution.

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

    hostname = map_uri_to_hostname(uri)
    if not hostname:
        return uri

    # Construct the netloc with the resolved hostname and the original port if present.
    netloc = hostname
    if port is not None:
        netloc += f":{port}"

    # Replace the netloc in the parsed URI with the constructed netloc.
    return parsed._replace(netloc=netloc).geturl()

### Flask app helper functions

def format_cell(value: str | int | datetime, expired: bool) -> str:
    """
    Format a table cell with optional expiration styling.

    Args:
        value (str | int | datetime): Text, an integer or a datetime timestamp
            to convert to text and HTML-escape.
        expired (bool): Whether the cell is expired.

    Returns:
        str: The HTML-formatted cell content.

    """
    text = escape(str(value))
    return f'<span style="color: red;">{text}</span>' if expired else text


def format_connection_type(connection_type: int) -> str:
    """
    Format the connection type as a human-readable string.

    Args:
        connection_type (int): The connection type identifier.

    Returns:
        str: The formatted connection type string.
    """
    # Example mapping, replace with actual connection type mapping if needed.
    connection_type_map = {
        0: "kSendRecv",
        1: "kPubSub",
    }
    return connection_type_map.get(connection_type, "Unknown")


def stats_to_html(
    dstream: TextIO,
    last_stats: datetime,
    npublishes: int,
    publish_time: timedelta,
    nlookups: int,
    lookup_time: timedelta,
    maxpartitions: int,
    maxentries: dict[str, int],
) -> None:
    """
    Render the server statistics into the provided HTML stream.

    Args:
        dstream (TextIO): The output stream to write the HTML content to.
        last_stats (datetime): The timestamp of the last statistics reset.
        npublishes (int): The number of publish calls made.
        publish_time (timedelta): The total time spent on publish calls.
        nlookups (int): The number of lookup calls made.
        lookup_time (timedelta): The total time spent on lookup calls.
        maxpartitions (int): The maximum number of partitions observed.
        maxentries (dict[str, int]): The maximum number of entries per partition.

    Returns:
        None

    """
    # Write the timestamp of the last statistics reset.
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
            f"<p>Maximum entries in partition {escape(part)} = {maxentries[part]}</p>"
        )
