#!/usr/bin/env python3
"""Burst-concurrency harness for Agent Substrate, mirroring agentenv-on-gke/tests/concurrency.py.

Runs inside the cluster as a Job. Drives the ateapi gRPC control plane directly (bearer token +
cluster trust bundle) and reaches each actor's ate-env guest through the atenet router
(`ate-target-actor` header) for readiness and the first command. One thread per actor, all
released together by a barrier; one JSON line per actor plus a summary line.

Modes (MODE):
  golden      N actors from N distinct templates whose goldens are ready (template start, per image)
  golden-one  N actors from ONE template (template start, one image; cache-warm after the first per node)
  cold        N fresh templates created just before the actors, so each actor cold-boots from its
              image (the golden is not ready yet); the concurrent golden bakes compete for workers
  pause       golden path, then pause all at once, settle, resume all at once (node-local snapshot)
  suspend     golden path, then suspend all at once (snapshot to GCS), settle, resume all at once

Env: ATEAPI, ROUTER, TOKEN_FILE, CA_FILE, ATESPACE, POOL_LABEL (label=value on the WorkerPool),
IMAGES (tsv: tag<TAB>digest[<TAB>size]), IMG_REPO, GUEST_IMAGE, BUCKET, MODE, N, CPU, MEM,
RUN_LABEL, TEMPLATE_PREFIX, GOLDEN_TIMEOUT_S, PAUSE_SETTLE_S, SANDBOX_CONFIG.
"""
import json, os, sys, time, threading, uuid
from concurrent.futures import ThreadPoolExecutor
import grpc, requests
from google.protobuf.json_format import MessageToDict, ParseDict
import ateapi_pb2 as A, ateapi_pb2_grpc as AG, guest_pb2 as G, guest_pb2_grpc as GG

ATEAPI = os.environ.get("ATEAPI", "api.ate-system.svc:443")
ROUTER = os.environ.get("ROUTER", "atenet-router.ate-system.svc:80")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "/var/run/ate/token"); CA_FILE = os.environ.get("CA_FILE", "/var/run/ate/ca.pem")
ATESPACE = os.environ.get("ATESPACE", "rl-bench"); POOL_LABEL = os.environ.get("POOL_LABEL", "workload=rl-bench-ateom")
IMG_REPO = os.environ.get("IMG_REPO", "us-docker.pkg.dev/gke-ai-eco-dev/swebench-mirror/swebench-verified")
GUEST_IMAGE = os.environ["GUEST_IMAGE"]; BUCKET = os.environ.get("BUCKET", "gs://glottman-snapshot-test/rl-bench/")
SANDBOX_CONFIG = os.environ.get("SANDBOX_CONFIG", "gvisor-default")
MODE = os.environ.get("MODE", "golden"); N = int(os.environ.get("N", "1"))
CPU = os.environ.get("CPU", "2"); MEM = os.environ.get("MEM", "2Gi"); LABEL = os.environ.get("RUN_LABEL", "")
PREFIX = os.environ.get("TEMPLATE_PREFIX", "swe"); GOLDEN_TIMEOUT = float(os.environ.get("GOLDEN_TIMEOUT_S", "1800"))
SETTLE = float(os.environ.get("PAUSE_SETTLE_S", "5")); HOST_SUFFIX = "actors.resources.substrate.ate.dev"
RUN = uuid.uuid4().hex[:6]

images = []
for line in open(os.environ.get("IMAGES", "/work/images.tsv")):
    if not line.strip() or line.startswith("#"): continue
    p = line.rstrip("\n").split("\t"); images.append((p[0], p[1]))
assert len(images) >= N or MODE == "golden-one", f"need {N} images, have {len(images)}"

# ---------------------------------------------------------------- control plane (ateapi)
class _Bearer(grpc.AuthMetadataPlugin):
    def __call__(self, ctx, cb):
        cb((("authorization", "Bearer " + open(TOKEN_FILE).read().strip()),), None)
creds = grpc.composite_channel_credentials(grpc.ssl_channel_credentials(open(CA_FILE, "rb").read()),
                                           grpc.metadata_call_credentials(_Bearer()))
chan = grpc.secure_channel(ATEAPI, creds, options=[("grpc.max_receive_message_length", 64 << 20)])
ctl = AG.ControlStub(chan)

def ref(name, atespace=ATESPACE): return A.ObjectRef(atespace=atespace, name=name)

def template_msg(name, tag, digest):
    """ActorTemplate: the task image unmodified, the ate-env guest mounted as an OCI image volume."""
    pool_key, pool_val = POOL_LABEL.split("=", 1)
    guest = "/ate/ko-app/ate-env-guest"
    d = {"metadata": {"atespace": ATESPACE, "name": name},
         "workerSelector": {"matchLabels": {pool_key: pool_val}},
         "containers": [{"name": "main", "image": f"{IMG_REPO}@{digest}",
                         "command": ["/bin/sh", "-c", f"exec {guest} -listen :80 -workspace /testbed"],
                         "wakeupProbe": {"httpGet": {"path": "/readyz", "port": 80}},
                         "volumeMounts": [{"name": "guest", "mountPath": "/ate"}]}],
         "volumes": [{"name": "guest", "image": {"reference": GUEST_IMAGE}}],
         "resources": {"limits": [{"name": "cpu", "quantity": CPU}, {"name": "memory", "quantity": MEM}]},
         "snapshotConfig": {"onPause": "SNAPSHOT_CONTENT_SCOPE_FULL", "onCommit": "SNAPSHOT_CONTENT_SCOPE_FULL",
                            "storageLocation": BUCKET},
         "sandboxConfig": {"sandboxClass": "SANDBOX_CLASS_GVISOR", "configName": SANDBOX_CONFIG}}
    return ParseDict(d, A.ActorTemplate())

def ensure_atespace():
    try: ctl.CreateAtespace(A.CreateAtespaceRequest(atespace=A.Atespace(metadata=A.ResourceMetadata(name=ATESPACE))))
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS: raise

def get_template(name):
    try: return ctl.GetActorTemplate(A.GetActorTemplateRequest(actor_template=ref(name)))
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND: return None
        raise

def ensure_template(name, tag, digest):
    """Create if missing. Returns (created_now: bool, create_epoch)."""
    if get_template(name): return False, None
    t0 = time.time()
    try: ctl.CreateActorTemplate(A.CreateActorTemplateRequest(actor_template=template_msg(name, tag, digest)))
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS: raise
    return True, t0

def golden_ready(name):
    t = get_template(name); st = t.status.golden_snapshot_status if t else None
    if st and st.error_message: return "error:" + st.error_message[:120]
    return "ready" if (st and st.golden_tag.name) else "pending"

def wait_goldens(names, timeout):
    t0 = time.monotonic(); pending = set(names); ready_at = {}; errors = {}
    while pending and time.monotonic() - t0 < timeout:
        for n in list(pending):
            s = golden_ready(n)
            if s == "ready": ready_at[n] = round(time.monotonic() - t0, 1); pending.discard(n)
            elif s.startswith("error"): errors[n] = s; pending.discard(n)
        if pending: time.sleep(2)
    return ready_at, errors, sorted(pending)

def actor_state(name):
    a = ctl.GetActor(A.GetActorRequest(actor=ref(name)))
    return A.ActorState.Name(a.status.state), a

def worker_of(a):
    wa = a.status.worker_assignment
    d = MessageToDict(wa) if a.status.HasField("worker_assignment") else {}
    return {k: v for k, v in d.items() if k != "worker"} | ({"worker": d.get("worker", {}).get("name")} if d else {})

def rpc_retry(fn, tries=40, base=0.25, retry_codes=(grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.ABORTED)):
    """Substrate answers FailedPrecondition/Unavailable while a worker is not free yet: that is queueing."""
    n = 0
    while True:
        try: return fn(), n
        except grpc.RpcError as e:
            if e.code() not in retry_codes or n >= tries: raise
            n += 1; time.sleep(min(5.0, base * (1.6 ** n)))

# ---------------------------------------------------------------- data path (router -> guest)
def actor_headers(name): return {"ate-target-actor": f"{ATESPACE}/{name}", "Host": f"{name}.{ATESPACE}.{HOST_SUFFIX}"}

def wait_readyz(name, deadline_s=600):
    t0 = time.monotonic(); last = ""
    while time.monotonic() - t0 < deadline_s:
        try:
            r = requests.get(f"http://{ROUTER}/readyz", headers=actor_headers(name), timeout=10)
            if r.status_code == 200: return True, ""
            last = f"http {r.status_code} {r.text[:80]}"
        except Exception as e:                  # noqa: BLE001
            last = f"{type(e).__name__}: {e}"[:120]
        time.sleep(0.25)
    return False, last

def guest_stub(name):
    ch = grpc.insecure_channel(ROUTER, options=[("grpc.default_authority", f"{name}.{ATESPACE}.{HOST_SUFFIX}")])
    return GG.ProcessServiceStub(ch), ch

def run_cmd(name, argv, timeout_s=180):
    stub, ch = guest_stub(name); md = (("ate-target-actor", f"{ATESPACE}/{name}"),)
    try:
        pid = stub.StartProcess(G.StartProcessRequest(command=argv, cwd="/"), metadata=md, timeout=60).process_id
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout_s:
            p = stub.GetProcess(G.GetProcessRequest(process_id=pid), metadata=md, timeout=30)
            if p.status != G.PROCESS_STATUS_RUNNING: return p.exit_code, G.ProcessStatus.Name(p.status)
            time.sleep(0.1)
        return -1, "timeout"
    finally: ch.close()

# ---------------------------------------------------------------- per-actor phases
start_barrier = threading.Barrier(N); pause_barrier = threading.Barrier(N); resume_barrier = threading.Barrier(N)
lock = threading.Lock(); actors = []

def arrive(b):
    try: b.wait(timeout=900)
    except threading.BrokenBarrierError: pass

def create_phase(name, template, rec):
    t0 = time.monotonic(); rec["t_start_epoch"] = time.time()
    _, n1 = rpc_retry(lambda: ctl.CreateActor(A.CreateActorRequest(actor=A.Actor(
        metadata=A.ResourceMetadata(atespace=ATESPACE, name=name), actor_template=ref(template)))))
    with lock: actors.append(name)
    rec["t_create"] = round(time.monotonic() - t0, 3)
    _, n2 = rpc_retry(lambda: ctl.ResumeActor(A.ResumeActorRequest(actor=ref(name))))
    rec["t_post"] = round(time.monotonic() - t0, 3); rec["retries"] = n1 + n2
    state = ""
    while time.monotonic() - t0 < 900:
        state, a = actor_state(name)
        if state == "ACTOR_STATE_RUNNING": rec["worker"] = worker_of(a); break
        if state == "ACTOR_STATE_CRASHED": break
        time.sleep(0.25)
    rec["t_running"] = round(time.monotonic() - t0, 3); rec["state"] = state
    if state != "ACTOR_STATE_RUNNING": rec["status"] = "not_running"; return False
    ok, why = wait_readyz(name); rec["t_ready"] = round(time.monotonic() - t0, 3)
    if not ok: rec["status"] = "not_ready"; rec["error"] = why; return False
    t1 = time.monotonic()
    code, st = run_cmd(name, ["/bin/sh", "-c", "echo ready && test -d /testbed && echo TESTBED_OK"])
    rec["t_first_cmd"] = round(time.monotonic() - t0, 3); rec["cmd_s"] = round(time.monotonic() - t1, 3)
    rec["status"] = "pass" if code == 0 else "cmd_failed"; rec["cmd_exit"] = code
    return code == 0

def park_phase(name, rec):
    """pause or suspend (by MODE) at the pause barrier, then resume at the resume barrier."""
    tp = time.monotonic()
    if MODE == "pause": _, r = rpc_retry(lambda: ctl.PauseActor(A.PauseActorRequest(actor=ref(name)))); target = "ACTOR_STATE_PAUSED"
    else: _, r = rpc_retry(lambda: ctl.SuspendActor(A.SuspendActorRequest(actor=ref(name)))); target = "ACTOR_STATE_SUSPENDED"
    rec["park_call_s"] = round(time.monotonic() - tp, 3)
    state = ""
    while time.monotonic() - tp < 600:
        state, _ = actor_state(name)
        if state == target: break
        time.sleep(0.2)
    rec["park_s"] = round(time.monotonic() - tp, 3); rec["park_state"] = state
    if state != target: rec["status"] = "park_failed"; return False
    return True

def resume_phase(name, rec):
    tr = time.monotonic(); rec["t_resume_start_epoch"] = time.time()
    _, r = rpc_retry(lambda: ctl.ResumeActor(A.ResumeActorRequest(actor=ref(name))))
    rec["resume_call_s"] = round(time.monotonic() - tr, 3); rec["resume_retries"] = r
    state = ""
    while time.monotonic() - tr < 900:
        state, a = actor_state(name)
        if state == "ACTOR_STATE_RUNNING": rec["resume_worker"] = worker_of(a); break
        if state == "ACTOR_STATE_CRASHED": break
        time.sleep(0.2)
    rec["resume_running_s"] = round(time.monotonic() - tr, 3)
    if state != "ACTOR_STATE_RUNNING": rec["status"] = "resume_not_running"; rec["state"] = state; return
    ok, why = wait_readyz(name); rec["resume_ready_s"] = round(time.monotonic() - tr, 3)
    if not ok: rec["status"] = "resume_not_ready"; rec["error"] = why; return
    tc = time.monotonic()
    code, st = run_cmd(name, ["/bin/sh", "-c", "echo back && test -d /testbed && echo TESTBED_OK"])
    rec["resume_first_cmd_s"] = round(time.monotonic() - tr, 3); rec["resume_cmd_s"] = round(time.monotonic() - tc, 3)
    if code != 0: rec["status"] = "resume_cmd_failed"; rec["cmd_exit"] = code

def one(i, template, tag):
    name = f"{PREFIX}-{RUN}-{i}"; rec = {"instance": tag, "template": template, "actor": name, "n": N, "label": LABEL, "mode": MODE}
    start_barrier.wait(timeout=300)
    ok = False
    try: ok = create_phase(name, template, rec)
    except Exception as e:                      # noqa: BLE001
        rec.setdefault("status", "error"); rec["error"] = f"{type(e).__name__}: {e}"[:300]
    if MODE not in ("pause", "suspend"): return rec
    arrive(pause_barrier); parked = False
    if ok:
        try: parked = park_phase(name, rec)
        except Exception as e:                  # noqa: BLE001
            rec["status"] = "error"; rec["error"] = f"{type(e).__name__}: {e}"[:300]
    time.sleep(SETTLE); arrive(resume_barrier)
    if parked:
        try: resume_phase(name, rec)
        except Exception as e:                  # noqa: BLE001
            rec["status"] = "error"; rec["error"] = f"{type(e).__name__}: {e}"[:300]
    return rec

def delete_actor(name):
    try: ctl.DeleteActor(A.DeleteActorRequest(actor=ref(name), any_state=True)); return "deleted"
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND: return "gone"
        try:
            rpc_retry(lambda: ctl.SuspendActor(A.SuspendActorRequest(actor=ref(name))), tries=10)
            for _ in range(300):
                if actor_state(name)[0] == "ACTOR_STATE_SUSPENDED": break
                time.sleep(0.5)
            ctl.DeleteActor(A.DeleteActorRequest(actor=ref(name))); return "suspended+deleted"
        except grpc.RpcError as e2: return f"delete_failed: {e2.code().name} {e2.details()[:80]}"

# ---------------------------------------------------------------- main
def pct(v, p):
    v = sorted(v); return round(v[min(len(v) - 1, int(round(p * (len(v) - 1))))], 2) if v else None

ensure_atespace()
setup = {"mode": MODE}; ts0 = time.monotonic()
if MODE == "golden-one":
    plan = [(f"{PREFIX}-one", images[0][0])] * N
    created, t0c = ensure_template(f"{PREFIX}-one", *images[0]); setup["templates_created"] = int(created)
    ready, errs, pend = wait_goldens([f"{PREFIX}-one"], GOLDEN_TIMEOUT)
elif MODE == "cold":
    plan = [(f"{PREFIX}-cold-{RUN}-{i}", images[i][0]) for i in range(N)]
    with ThreadPoolExecutor(max_workers=min(N, 50)) as ex:
        list(ex.map(lambda p: ensure_template(p[1][0], *p[0]), zip(images[:N], plan)))
    setup["templates_created"] = N; ready, errs, pend = {}, {}, []
else:
    plan = [(f"{PREFIX}-{i}", images[i][0]) for i in range(N)]
    with ThreadPoolExecutor(max_workers=min(N, 50)) as ex:
        made = list(ex.map(lambda p: ensure_template(p[1][0], *p[0])[0], zip(images[:N], plan)))
    setup["templates_created"] = sum(1 for m in made if m)
    ready, errs, pend = wait_goldens([t for t, _ in plan], GOLDEN_TIMEOUT)
setup["setup_s"] = round(time.monotonic() - ts0, 1); setup["goldens_ready"] = len(ready); setup["golden_errors"] = errs; setup["goldens_pending"] = pend
if ready and setup["templates_created"]: setup["golden_ready_s"] = {"p50": pct(list(ready.values()), .5), "p90": pct(list(ready.values()), .9), "max": pct(list(ready.values()), 1.0)}
print(json.dumps({"setup": setup}), flush=True)
if MODE == "cold": time.sleep(1)

with ThreadPoolExecutor(max_workers=N) as ex:
    results = list(ex.map(lambda ip: one(ip[0], ip[1][0], ip[1][1]), enumerate(plan)))
time.sleep(5)
kill_t0 = time.monotonic()
with ThreadPoolExecutor(max_workers=20) as ex: kills = list(ex.map(delete_actor, actors))
kill_s = round(time.monotonic() - kill_t0, 1)
if MODE == "cold":
    for t, _ in plan:
        try: ctl.DeleteActorTemplate(A.DeleteActorTemplateRequest(actor_template=ref(t)))
        except grpc.RpcError: pass
for r in results: print(json.dumps(r), flush=True)
ok = [r for r in results if r.get("status") == "pass"]
per_node = {}
for r in ok:
    k = (r.get("worker") or {}).get("workerNodeName") or (r.get("worker") or {}).get("nodeName") or "?"; per_node[k] = per_node.get(k, 0) + 1
summ = {"summary": True, "system": "substrate", "mode": MODE, "n": N, "label": LABEL, "pass": len(ok), "fail": N - len(ok),
        "cpu": CPU, "mem": MEM, "template_setup": setup, "per_node": per_node, "kill_all_s": kill_s,
        "kill_outcomes": {k: kills.count(k) for k in set(kills)}, "retries_total": sum(r.get("retries", 0) for r in results)}
keys = ["t_create", "t_post", "t_running", "t_ready", "t_first_cmd", "cmd_s"]
if MODE in ("pause", "suspend"): keys += ["park_call_s", "park_s", "resume_call_s", "resume_running_s", "resume_ready_s", "resume_first_cmd_s", "resume_cmd_s"]
for k in keys:
    v = [r[k] for r in ok if k in r]; summ[k] = {"p50": pct(v, .5), "p90": pct(v, .9), "max": pct(v, 1.0), "min": pct(v, 0.0)}
summ["failures"] = [{k: r.get(k) for k in ("instance", "actor", "status", "state", "error", "park_state", "cmd_exit")} for r in results if r.get("status") != "pass"][:20]
print(json.dumps(summ), flush=True)
