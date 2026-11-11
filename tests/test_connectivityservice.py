import json
from unittest.mock import MagicMock

import pytest

import connectivityserver.connectionflask as cf
from connectivityserver import utils


@pytest.fixture()
def app():
    yield cf.app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def runner(app):
    return app.test_cli_runner()


con = json.loads("""{
   "connections":[
    {
     "connection_type":0,
     "data_type":"TPSet",
     "uid":"DRO-000-tp_to_trigger",
     "uri":"tcp://192.168.1.100:1234"
    },
    {
     "connection_type":0,
     "data_type":"TPSet",
     "uid":"DRO-001-tp_to_trigger",
     "uri":"tcp://192.168.1.100:1235"
    }
   ],
   "partition":"ccTest"
  }""")


def test_noconnection(client):
    resp = client.get("/getconnection/bad/con")
    assert resp.status_code == 404


def test_publish(client):
    resp = client.post("/publish", json=con)
    assert resp.status_code == 200


def test_lookup(client):
    query = json.loads("""{"uid_regex":"DRO.*", "data_type":"TPSet"}""")
    resp = client.post("/getconnection/ccTest", json=query)
    assert resp.status_code == 200

    rjson = json.loads(resp.data)
    assert len(rjson) == 2
    assert rjson[0]["uid"] == "DRO-000-tp_to_trigger"
    assert rjson[1]["uid"] == "DRO-001-tp_to_trigger"

    query = json.loads("""{"uid_regex":"DUMMY.*", "data_type":"TPSet"}""")
    resp = client.post("/getconnection/ccTest", json=query)
    assert resp.status_code == 200
    rjson = json.loads(resp.data)
    assert len(rjson) == 0


def test_retract(client):
    resp = client.post("/retract")
    assert resp.status_code == 400

    retraction = json.loads("""{"partition":"ccTest",
    "connections":[{"connection_id":"DRO-000-tp_to_trigger"},
                   {"connection_id":"DRO-001-tp_to_trigger"}]
  }""")
    resp = client.post("/retract", json=retraction)
    assert resp.status_code == 200

    query = json.loads("""{"uid_regex":"DRO.*", "data_type":"TPSet"}""")
    resp = client.post("/getconnection/ccTest", json=query)
    assert resp.status_code == 404

    # Second time should fail
    resp = client.post("/retract", json=retraction)
    assert resp.status_code == 404


def test_retract_partition(client):
    resp = client.post("/publish", json=con)
    assert resp.status_code == 200

    resp = client.post("/retract-partition")
    assert resp.status_code == 400

    retraction = json.loads("""{"partition":"ccTest"}""")
    resp = client.post("/retract-partition", json=retraction)
    assert resp.status_code == 200

    # Second time should fail
    resp = client.post("/retract-partition", json=retraction)
    assert resp.status_code == 404


def test_dump(client):
    resp = client.get("/")
    assert b"Dump" in resp.data


def test_stats(client):
    resp = client.get("/stats")
    assert resp.status_code == 200
    assert b"<h1>Connection server statistics" in resp.data
    assert b"<p>0 calls to publish" not in resp.data

    resp = client.get("/resetStats")
    assert resp.status_code == 200
    assert b"<h1>Connection server statistics" in resp.data
    assert b"<p>0 calls to publish" not in resp.data

    resp = client.get("/stats")
    assert resp.status_code == 200
    assert b"<h1>Connection server statistics" in resp.data
    assert b"<p>0 calls to publish" in resp.data


def test_reset(client):
    resp = client.post("/publish", json=con)
    assert resp.status_code == 200

    resp = client.get("/stats")
    assert resp.status_code == 200
    assert b"<p>0 calls to publish" not in resp.data

    resp = client.get("/resetService")
    assert resp.status_code == 200

    resp = client.get("/stats")
    assert resp.status_code == 200
    assert b"<p>0 calls to publish" in resp.data


def test_live(client):
    resp = client.get("/live")
    assert resp.status_code == 200
    assert b"OK" in resp.data


def test_ready(client):
    resp = client.post("/publish", json=con)
    assert resp.status_code == 200
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert b"OK" in resp.data


@pytest.fixture()
def control_plane(monkeypatch):
    """Provide a mock control plane without reading or changing real kubeconfig."""
    monkeypatch.setenv("KUBECONFIG", "/test/kubeconfig")
    loader = MagicMock()
    monkeypatch.setattr(utils.config, "load_kube_config", loader)
    monkeypatch.setattr(utils.client, "ApiClient", MagicMock())
    api = MagicMock()
    monkeypatch.setattr(utils.client, "CoreV1Api", MagicMock(return_value=api))
    api.list_pod_for_all_namespaces.return_value.items = [
        utils.client.V1Pod(
            spec=utils.client.V1PodSpec(containers=[], node_name="node-id")
        )
    ]
    api.read_node.return_value = utils.client.V1Node(
        status=utils.client.V1NodeStatus(
            addresses=[
                utils.client.V1NodeAddress(type="Hostname", address="physical-host")
            ]
        )
    )
    return api, loader


@pytest.mark.parametrize("kubeconfig", [None, ""])
def test_no_kubeconfig_uses_dns_without_loading_config(monkeypatch, kubeconfig):
    """Missing or empty KUBECONFIG skips cluster access and preserves local DNS."""
    if kubeconfig is None:
        monkeypatch.delenv("KUBECONFIG", raising=False)
    else:
        monkeypatch.setenv("KUBECONFIG", kubeconfig)
    loader = MagicMock()
    monkeypatch.setattr(utils.config, "load_kube_config", loader)
    monkeypatch.setattr(utils, "resolve_hostname", lambda host: "local.example.org")
    debug = MagicMock()
    monkeypatch.setattr(utils.log, "debug", debug)
    assert (
        utils.resolve_uri_hostname("tcp://131.225.193.20:1234") == "local.example.org"
    )
    assert utils.resolve_pod_hostname("131.225.193.20") == ""
    loader.assert_not_called()
    debug.assert_any_call("Skipping cluster lookup: KUBECONFIG is not set")


def test_local_dns_precedes_successful_cluster_lookup(monkeypatch, control_plane):
    """Try local DNS before the API, then prefer the physical node hostname."""
    api, loader = control_plane
    events = []

    def dns(host):
        events.append(host)
        return (
            "local.example.org" if host == "131.225.193.20" else "physical.example.org"
        )

    monkeypatch.setattr(utils, "resolve_hostname", dns)
    response = api.list_pod_for_all_namespaces.return_value

    def list_pods(**kwargs):
        assert events == ["131.225.193.20"]
        assert kwargs["field_selector"] == "status.podIP=131.225.193.20"
        return response

    api.list_pod_for_all_namespaces.side_effect = list_pods
    assert (
        utils.resolve_uri_hostname("tcp://131.225.193.20:1234")
        == "physical.example.org"
    )
    assert loader.call_args.kwargs["config_file"] == "/test/kubeconfig"
    assert loader.call_args.kwargs["persist_config"] is False


@pytest.mark.parametrize(
    "failure", ["config", "api", "transport", "no-pod", "node-hostname"]
)
def test_cluster_failure_keeps_local_dns(monkeypatch, control_plane, failure):
    """Unavailable config/API/pod/node hostname is debug-logged and keeps DNS."""
    api, loader = control_plane
    if failure == "config":
        loader.side_effect = utils.ConfigException("No configuration found")
    elif failure == "api":
        api.list_pod_for_all_namespaces.side_effect = utils.client.ApiException(
            status=403
        )
    elif failure == "transport":
        api.list_pod_for_all_namespaces.side_effect = utils.HTTPError(
            "Connection failed"
        )
    elif failure == "no-pod":
        api.list_pod_for_all_namespaces.return_value.items = []
    else:
        api.read_node.return_value.status.addresses = []
    monkeypatch.setattr(utils, "resolve_hostname", lambda host: "local.example.org")
    debug, warning = MagicMock(), MagicMock()
    monkeypatch.setattr(utils.log, "debug", debug)
    monkeypatch.setattr(utils.log, "warning", warning)
    assert (
        utils.resolve_uri_hostname("tcp://131.225.193.20:1234") == "local.example.org"
    )
    assert debug.called
    warning.assert_not_called()


def test_failed_local_dns_can_use_cluster(monkeypatch, control_plane):
    """A missing local DNS result does not prevent physical-host discovery."""
    monkeypatch.setattr(utils, "resolve_hostname", lambda host: "")
    assert utils.resolve_uri_hostname("tcp://131.225.193.20:1234") == "physical-host"


def test_cluster_dns_is_not_a_physical_hostname_without_config(monkeypatch):
    """Never display a service DNS name as a physical host when cluster access is skipped."""
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.setattr(
        utils, "resolve_hostname", lambda host: "endpoint.ns.svc.cluster.local"
    )
    assert utils.resolve_uri_hostname("tcp://endpoint.ns.svc.cluster.local:1234") == ""


def test_cluster_dns_can_resolve_physical_host(monkeypatch, control_plane):
    """Resolve a cluster name to its IP and then use the scheduled node's hostname."""
    api, _ = control_plane
    monkeypatch.setattr(
        utils,
        "resolve_hostname",
        lambda host: (
            "endpoint.ns.svc.cluster.local"
            if ".svc." in host
            else "physical.example.org"
        ),
    )
    monkeypatch.setattr(
        utils.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, "", ("10.244.1.2", 0))],
    )
    assert (
        utils.resolve_uri_hostname("tcp://endpoint.ns.svc.cluster.local:1234")
        == "physical.example.org"
    )
    api.list_pod_for_all_namespaces.assert_called_once_with(
        field_selector="status.podIP=10.244.1.2",
        _request_timeout=(3, 3),
    )


def test_failed_resolution_retains_raw_uri(monkeypatch):
    """An unavailable cluster and failed DNS leave the original display URI intact."""
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.setattr(utils, "resolve_hostname", lambda host: "")
    monkeypatch.setattr(utils, "hostname_cache", utils.OrderedDict())
    uri = "tcp://131.225.193.20:1234/path?key=value#fragment"
    assert utils.map_uri_to_resolved_uri(uri) == uri
