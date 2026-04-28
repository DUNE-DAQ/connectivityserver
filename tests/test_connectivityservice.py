import json
import pytest

import connectivityserver.connectionflask as cf

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
   "session":"ccTest"
  }""")



def test_noconnection(client):
  resp = client.get("/getconnection/bad/con")
  assert resp.status_code == 404

def test_publish(client):
  resp = client.post("/publish", json=con)
  assert resp.status_code == 200


def test_publish_with_partition_still_works(client):
  partition_con = {
    "connections": con["connections"],
    "partition": "ccPartitionCompat",
  }

  resp = client.post("/publish", json=partition_con)
  assert resp.status_code == 200

  query = {"uid_regex": "DRO.*", "data_type": "TPSet"}
  resp = client.post("/getconnection/ccPartitionCompat", json=query)
  assert resp.status_code == 200

  rjson = json.loads(resp.data)
  assert len(rjson) == 2

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

  retraction = json.loads("""{"session":"ccTest",
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


def test_retract_session(client):
  resp = client.post("/publish", json=con)
  assert resp.status_code == 200

  resp = client.post("/retract-session")
  assert resp.status_code == 400

  retraction = json.loads("""{"session":"ccTest"}""")
  resp = client.post("/retract-session", json=retraction)
  assert resp.status_code == 200

  # Second time should fail
  resp = client.post("/retract-session", json=retraction)
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
