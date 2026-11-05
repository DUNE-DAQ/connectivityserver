# This is part of the DUNE DAQ software, copyright 2020.
# Licensing/copyright details are in the COPYING file.

"""Non-Flask helpers for URI resolution, caching, logging and HTML rendering."""

import logging
import os
import re
import socket
import time
from collections import OrderedDict, namedtuple
from datetime import timedelta
from html import escape
from ipaddress import ip_address as parse_ip_address
from threading import RLock
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


def map_uri_to_hostname(uri: str) -> str:
    """
    Map a URI to a hostname, preferring a fully qualified name.

    Args:
        uri (str): The URI string to be mapped.

    Returns:
        str: The resolved hostname or an empty string if resolution fails.
    """
    with hostname_cache_lock:
        entry = hostname_cache.get(uri)
        if entry is not None:
            hostname, expires = entry
            if time.monotonic() < expires:
                hostname_cache.move_to_end(uri)
                return hostname
            del hostname_cache[uri]

    hostname = resolve_uri_hostname(uri)
    with hostname_cache_lock:
        hostname_cache[uri] = (hostname, time.monotonic() + 3600)
        hostname_cache.move_to_end(uri)
        while len(hostname_cache) > 4096:
            hostname_cache.popitem(last=False)
    return hostname


def resolve_uri_hostname(uri: str) -> str:
    """
    Resolve pod endpoints through the control plane and other hosts through DNS.

    A missing or malformed hostname uses the local outbound IP instead.

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
        log.debug(f"Could not parse {uri}, using the local outbound IP")
        return resolve_local_hostname()

    try:
        address = str(parse_ip_address(ip_address))
    except ValueError:
        if not re.search(r"\.(?:svc|pod)\.", ip_address, re.IGNORECASE):
            return resolve_hostname(ip_address)
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
            log.warning("Could not resolve pod endpoint %s: %s", ip_address, error)
            return ""
        if len(addresses) != 1:
            log.warning("Pod endpoint %s does not identify one IP", ip_address)
            return ""
        return resolve_pod_hostname(addresses.pop())[1]

    is_pod, hostname = resolve_pod_hostname(address)
    if is_pod:
        return hostname
    hostname = resolve_hostname(address)
    if re.search(r"\.(?:svc|pod)\.", hostname, re.IGNORECASE):
        log.warning("No physical hostname established for cluster endpoint %s", address)
        return ""
    return hostname


def resolve_pod_hostname(pod_ip: str):
    """
    Return (is_pod, hostname) from pod scheduling and the node's Hostname address.

    Read kubeconfig without persisting changes, not DRUNC_HOST_NAME or local discovery.
    None for is_pod means discovery failed; False means no matching pod exists.
    A matched pod without one node hostname returns (True, "").
    """
    try:
        address = str(parse_ip_address(pod_ip))
        configuration = client.Configuration()
        config.load_kube_config(
            client_configuration=configuration, persist_config=False
        )
        api_host = urlparse(configuration.host).hostname
        configuration.no_proxy = ",".join(
            filter(None, [configuration.no_proxy, api_host])
        )
        with client.ApiClient(configuration) as api_client:
            api = client.CoreV1Api(api_client)
            pods = api.list_pod_for_all_namespaces(
                field_selector=f"status.podIP={address}",
                _request_timeout=(3, 3),
            ).items
            if not pods:
                return False, ""
            nodes = {pod.spec.node_name for pod in pods}
            if None in nodes or "" in nodes or len(nodes) != 1:
                raise ValueError("pod IP does not identify one scheduled node")
            node = api.read_node(nodes.pop(), _request_timeout=(3, 3))
            names = {
                entry.address
                for entry in (node.status.addresses or [])
                if entry.type == "Hostname"
            }
        if len(names) != 1:
            raise ValueError("node does not report exactly one Hostname")
        hostname = names.pop()
        if not hostname or re.fullmatch(r"[A-Za-z0-9.-]+", hostname) is None:
            raise ValueError("invalid node hostname")
        return True, resolve_hostname(hostname) or hostname
    except client.ApiException as error:
        log.warning(
            "Control-plane lookup for %s failed (HTTP %s); check kubeconfig context, "
            "network access and permissions to list pods and get nodes",
            pod_ip,
            error.status,
        )
    except (OSError, HTTPError, ConfigException, ValueError) as error:
        log.warning("Could not identify physical pod host for %s: %s", pod_ip, error)
    return None, ""


def resolve_local_hostname() -> str:
    """
    Resolve this server's outbound IP using the system resolver.

    Returns:
        str: The local hostname, or an empty string if no suitable name is found.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(0.1)
            s.connect(("10.255.255.255", 1))
            ip_address = s.getsockname()[0]
    except OSError as e:
        log.warning("Could not determine the local outbound IP: %s", e)
        return ""

    return resolve_hostname(ip_address)


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
            for _, _, _, canonical, _ in addresses:
                if "." in canonical.rstrip("."):
                    return canonical.rstrip(".")
            for _, _, _, _, sockaddr in addresses:
                try:
                    hostname, aliases, _ = socket.gethostbyaddr(sockaddr[0])
                    for candidate in [hostname] + aliases:
                        if "." in candidate.rstrip("."):
                            return candidate.rstrip(".")
                except OSError:
                    continue
            log.warning("Could not obtain an FQDN for host %s", ip_address)
            return ""
        for candidate in [hostname] + aliases:
            if "." in candidate.rstrip("."):
                return candidate.rstrip(".")
        return hostname.rstrip(".")
    except OSError as e:
        log.warning("Could not resolve host %s: %s", ip_address, e)
        return ""


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


def format_cell(value, expired):
    text = escape(str(value))
    return f'<span style="color: red;">{text}</span>' if expired else text


def stats_to_html(
    dstream,
    last_stats,
    npublishes,
    publish_time,
    nlookups,
    lookup_time,
    maxpartitions,
    maxentries,
) -> None:
    """
    Render the server statistics into the provided HTML stream.

    This includes information about publish and lookup calls, their timings,
    and the maximum number of partitions and entries observed.

    The caller supplies the app's statistics while holding its state lock.
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
