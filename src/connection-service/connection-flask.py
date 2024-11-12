#
# @file connection-flask.py Simple prototype connection configuration server
# This is part of the DUNE DAQ software, copyright 2020.
#  Licensing/copyright details are in the COPYING file that you should have
#  received with this code.
#

import os
import json
import re
from threading import Lock
from io import StringIO
from datetime import datetime, timedelta
from collections import namedtuple
from flask import Flask, request, abort, make_response


sessions={}
session_lock=Lock()

if 'CONNECTION_FLASK_DEBUG' in os.environ:
  debug_level=int(os.environ['CONNECTION_FLASK_DEBUG'])
else:
  debug_level=0

if 'ENTRY_TTL' in os.environ:
  ttl=int(os.environ['ENTRY_TTL'])
else:
  ttl=10
entry_ttl=timedelta(seconds=ttl)

last_stats=datetime.now()
npublishes=0
nlookups=0
lookup_time=timedelta(0)
publish_time=timedelta()
maxsessions=0
maxentries={}

app=Flask(__name__)

@app.route("/")
def dump():
  now=datetime.now()
  dstream=StringIO()
  dstream.write(f'<h1>Dump of configuration dictionary</h1>')
  dstream.write(f"<h2>Active sessions</h2><p>")
  if len(sessions)>0:
    pad=' style="padding-left: 1em;padding-right: 1em"'
    dstream.write(f'<table style="border: 1px solid black">'
                  f'<tr style="background: #e0e0e0"><th{pad}>Session</th>'
                  f'<th{pad}>Entries</th></tr>')
    for p in sessions:
      dstream.write(f'<tr><td{pad}>{p}'
                    f'</td><td{pad}>{len(sessions[p])}</td></tr>')
    dstream.write(f"</table>")
    for p in sessions:
      store=sessions[p]
      dstream.write(f'<h2>Session {p}</h2><p>')
      for k,v in store.items():
        if now-v.time<entry_ttl:
          dstream.write(f'{k}: {v}</br>')
        else:
          dstream.write(f'<strike>{k}: {v}</strike></br>')
      dstream.write("</p>")
  else:
    dstream.write(f"None</p>")
  dstream.write(f"<hr><h2>Server statistics</h2>")
  stats_to_html(dstream)
  dstream.seek(0)
  return dstream.read()

def stats_to_html(dstream):
  dstream.write(f"<p>Since {last_stats}</p>")
  if npublishes>0:
    avg_pub=publish_time/npublishes
  else:
    avg_pub=timedelta()
  dstream.write(f"<p>{npublishes} calls to publish in total time {publish_time} "
                f"(average {avg_pub.microseconds} &micro;s per call)</p>")
  if nlookups>0:
    avg_lookup=lookup_time/nlookups
  else:
    avg_lookup=timedelta()
  dstream.write(f"<p>{nlookups} calls to lookup in total time {lookup_time} "
                f"(average {avg_lookup.microseconds} &micro;s per call)</p>")
  dstream.write(f"<p>Maximum number of sessions active = {maxsessions}</p>")
  for session in maxentries:
    dstream.write(f"<p>Maximum entries in session {session} = {maxentries[session]}</p>")

@app.route("/stats")
def dumpStats():
  dstream=StringIO()
  dstream.write(f'<h1>Connection server statistics</h1>')
  stats_to_html(dstream)
  dstream.seek(0)
  return dstream.read()

@app.route("/resetStats")
def resetStats():
  stats = dumpStats()

  global last_stats,npublishes,nlookups,lookup_time,publish_time,maxsessions,maxentries

  last_stats=datetime.now()
  npublishes=0
  nlookups=0
  lookup_time=timedelta(0)
  publish_time=timedelta()
  maxsessions=0
  maxentries={}

  return stats

@app.route("/resetService")
def reset():

  global sessions
  sessions={}
  return resetStats()


@app.route("/publish",methods=['POST'])
def publish():
  #  Store multiple connection ids and corresponding uris in a
  #  dictionary associated with the appropriate session.
  timestamp=datetime.now()
  js=json.loads(request.data)
  if debug_level>2:
    print (f"[{timestamp}] Publish {js=}")
  session = js.get('session')
  if session is None:
    session = js.get('partition')

  if debug_level>1:
    print(f"[{timestamp}] Publish {len(js['connections'])} connections in session {session}"
          f" from {request.remote_addr} uri={js['connections'][0]['uri']} ...")
  session_lock.acquire()
  if session in sessions:
    store=sessions[session]
  else:
    store={}
    sessions[session]=store
    global maxsessions
    if len(sessions)>maxsessions:
      maxsessions=len(sessions)
    if not session in maxentries:
      #print(f"Setting maxentries[{session}] to 0")
      maxentries[session]=0

  Connection=namedtuple(
    'Connection',['uri','data_type','connection_type','time']
  )

  for connection in js['connections']:
    #print (f"{connection=}")
    if 'uid' in  connection and 'uri' in connection:
      uid=connection['uid']
      store[uid]=Connection(uri=connection['uri'],
                            connection_type=connection['connection_type'],
                            data_type=connection['data_type'],
                            time=timestamp)
  now=datetime.now()
  elapsed=now-timestamp
  if debug_level>0:
    print(f"[{now}] Publish took {elapsed.microseconds} us to add {len(js['connections'])} connections")
  global npublishes, publish_time
  publish_time+=elapsed
  npublishes+=1
  if len(store)>maxentries[session]:
    maxentries[session]=len(store)

  session_lock.release()
  return 'OK'

@app.route("/retract-session",methods=['POST'])
@app.route("/retract-partition",methods=['POST'])
def retract_session():
  if debug_level>2:
    print(f"[datetime.now()] retract_session() request=[{request.form}]")

  session=request.form.get('session')
  if session is None:
    session=request.form.get('partition')
  if session in None:
    abort(400)

  session_lock.acquire()
  if session in sessions:
    sessions.pop(session)
    session_lock.release()
    return 'OK'
  else:
    session_lock.release()
    abort(404)

@app.route("/retract",methods=['POST'])
def retract():
  js=json.loads(request.data)
  good=True

  session=js.get('session')
  if session is None:
    session=js.get('partition')

  session_lock.acquire()
  if session not in sessions:
    session_lock.release()
    return make_response(f"session {session} not found", 404)

  store=sessions[session]
  for con in js['connections']:
    #print (f"{con=}")
    id=con['connection_id']
    if id in store:
      store.pop(id)
    else:
      print(f"retract() could not find connection_id <{id}>")
      good=False
  if len(store)==0:
    # We've deleted the last entry in this session so delete the
    # session as well
    sessions.pop(session)
  session_lock.release()
  if good:
    return 'OK'
  else:
    abort(404)

@app.route("/getconnection/<session>",methods=['POST','GET'])
@app.route("/getconnection/<partition>",methods=['POST','GET'])
def get_connection(session=None, partition=None):
  # Find connection uris that correspond to the connection id pattern
  # in the request. The pattern is treated as a regular expression.
  if session is None:
    session=partition
  if session is None:
    abort(400)

  now=datetime.now()
  js=json.loads(request.data)
  if debug_level>2:
    print (f"[{now}] get_connection() {js=}")

  if 'uid_regex' in js and 'data_type' in js:
    if debug_level>1:
      print(f"[{now}] get_connection()"
            f" Searching for connections matching uid_regex<{js['uid_regex']}>"
            f" and data_type {js['data_type']}")
    result=[]
    regex=re.compile(js['uid_regex'])
    dt=js['data_type']
    session_lock.acquire()

    if session in sessions:
      store=sessions[session]
      matched=[]
      for uid,con in store.items():
        if regex.fullmatch(uid) and con.data_type==dt and now-con.time<entry_ttl:
          #print (f"Found matching entry {uid} {con=}")
          #result.append('{'
          #              f'"uid":"{uid}",'
          #              f'"uri":"{con.uri}",'
          #              f'"connection_type":{con.connection_type},'
          #              f'"data_type":"{con.data_type}"'
          #              '}')
          matched.append((uid,con))
      session_lock.release()
      # We should now be able to construct JSON string while other threads
      # have access to the session dict
      for uid,con in matched:
        result.append('{'
                      f'"uid":"{uid}",'
                      f'"uri":"{con.uri}",'
                      f'"connection_type":{con.connection_type},'
                      f'"data_type":"{con.data_type}"'
                      '}')
      td=datetime.now()-now
      if debug_level>0:
        print(f"[{now}] get_connection() "
              f"Lookup took {td.microseconds} us to find {len(result)} connections")
      global nlookups, lookup_time
      # Should we have the lock while updating statistics? It doesn't
      # really matter if the stats aren't entirely accurate.
      nlookups+=1
      lookup_time+=td
      return "["+",".join(result)+"]"
    else:
      session_lock.release()
      print(f"[{now}] get_connection() Session {session} not found")
      abort(404)
  else:
    abort(400)

