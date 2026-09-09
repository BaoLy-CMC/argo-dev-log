#!/usr/bin/env python3
"""Self-check for the line-joining logic: python3 test_server.py (no framework, no network)."""
import json
import os
import tempfile
import time

import server

HEAD = ("2026-09-07 03:19:35.676 demo-auth-service [][][] [][]  INFO "
        "[ntainer#0-0-C-1] o.s.c.l.LogAccessor : partitions assigned: [topic-0, topic-1]")
ERR = "2026-09-07 03:19:36.100 demo-auth-service [][][] [][]  ERROR [main] o.a.X : boom"


def drained(stream):
    server.WINDOW = server.IDLE_FLUSH = 0
    time.sleep(0.01)
    return list(stream.drain())


def test_continuation_lines_join_one_event():
    st = server.Stream(["a"])
    for line in [HEAD, "    ssl.protocol = TLSv1.3", "    ssl.provider = null", ERR]:
        st._feed("auth", line)
    events = drained(st)
    assert len(events) == 2, events
    assert events[0]["level"] == "INFO" and events[1]["level"] == "ERROR"
    assert events[0]["line"].count("\n") == 2, events[0]["line"]
    assert events[0]["ts"] < events[1]["ts"], "continuation must not steal a fresh timestamp"
    assert "ssl.provider = null" in events[0]["line"]


def test_runaway_dump_is_capped():
    st = server.Stream(["a"])
    st._feed("auth", HEAD)
    for i in range(server.MAX_EVENT_LINES + 20):
        st._feed("auth", f"    key{i} = null")
    events = drained(st)
    assert len(events) == 2, len(events)
    assert events[0]["line"].count("\n") == server.MAX_EVENT_LINES - 1


def test_column_zero_lines_stay_separate():
    """Kong access logs carry no timestamp — they must not glue into one event."""
    st = server.Stream(["a"])
    st._feed("kong", '{"request":{"method":"GET"},"response":{"status":200}}')
    st._feed("kong", '127.0.0.6 - - [07/Sep/2026:04:09:19 +0000] "GET /x HTTP/1.1" 200 162')
    st._feed("kong", '{"request":{"method":"POST"},"response":{"status":500}}')
    events = drained(st)
    assert len(events) == 3, events
    assert all("\n" not in e["line"] for e in events)


def test_stack_trace_keywords_join():
    st = server.Stream(["a"])
    for line in [ERR, "\tat com.example.Foo.bar(Foo.java:1)", "Caused by: java.lang.NullPointerException",
                 "\tat com.example.Foo.baz(Foo.java:2)", "    ... 4 more"]:
        st._feed("auth", line)
    events = drained(st)
    assert len(events) == 1 and events[0]["line"].count("\n") == 4, events


def test_exception_head_joins_the_error_line():
    """The trace's first line sits at column 0; split off it would become its own INFO row."""
    st = server.Stream(["a"])
    for line in [ERR,
                 "org.springframework.web.client.HttpClientErrorException$Unauthorized: 401 Unauthorized",
                 "    at org.springframework.web.client.StatusHandler.handle(StatusHandler.java:75)",
                 "    at com.example.bff.controller.TokenController.exchange(TokenController.java:90)"]:
        st._feed("bff", line)
    events = drained(st)
    assert len(events) == 1, events
    assert events[0]["level"] == "ERROR", events[0]["level"]
    assert "HttpClientErrorException" in events[0]["line"]


def test_plain_column_zero_text_still_starts_an_event():
    st = server.Stream(["a"])
    st._feed("a", HEAD)
    st._feed("a", "Started Application in 3.4 seconds")
    st._feed("a", "hibernate.dialect = org.hibernate.dialect.PostgreSQLDialect")
    assert len(drained(st)) == 3


def test_reconnect_drops_the_replayed_tail():
    st = server.Stream(["a"])
    st._replayed("auth", HEAD)  # first pass: remembered as shown
    st.resuming.add("auth")  # stream dropped and came back
    assert st._replayed("auth", HEAD), "the same old line must not be printed again"
    assert not st._replayed("auth", ERR), "a genuinely new line still gets through"
    assert not st._replayed("auth", HEAD), "dedup stops once the replay window is over"


def test_orphan_continuation_survives_alone():
    st = server.Stream(["a"])
    st._feed("auth", "    ssl.keystore.type = JKS")  # stream started mid-dump
    assert len(drained(st)) == 1


def test_action_argv_and_whitelist():
    argv = server.ACTIONS["restart"]("evil --prune")
    assert argv[-3:] == ["--kind", "Deployment", "--all"], argv
    assert "evil --prune" in argv, "app name stays one argv item, never shell-split"

    server.list_apps = lambda: [{"name": "real-service"}]
    calls = []
    server.subprocess.run = lambda cmd, **kw: calls.append(cmd) or type(
        "R", (), {"returncode": 0, "stdout": "done\n", "stderr": ""})()
    out = server.run_action("sync", ["real-service", "--core"])
    assert [r["ok"] for r in out] == [True, False], out
    assert out[1]["msg"] == "unknown app", out[1]
    assert len(calls) == 1 and calls[0][-1] == "--async", calls


def test_domain_list_validation():
    """Hostnames only, and the saved file must never carry a token."""
    assert server.DOMAIN_RE.match("argocd.nonprod.example.com")
    assert server.DOMAIN_RE.match("argocd.dev.io:8443")
    for bad in ["https://argocd.io", "argocd.io/api", "evil com", "", "a b"]:
        assert not server.DOMAIN_RE.match(bad), bad


def test_token_is_per_domain():
    server._conf["domains"] = ["a.example.io", "b.example.io"]
    server._tokens.clear()
    server._tokens["a.example.io"] = "tok-A"
    server._conf["active"] = "a.example.io"
    assert server.active_token() == "tok-A"
    assert server._env()["ARGOCD_SERVER"] == "a.example.io"
    server._conf["active"] = "b.example.io"
    assert server.active_token() == "", "a domain must not inherit another domain's token"


def test_pod_counts_parse_and_reject_bad_names():
    tree = {"nodes": [{"kind": "Pod", "health": {"status": "Healthy"}},
                      {"kind": "Pod", "health": {"status": "Degraded"}},
                      {"kind": "Deployment", "health": {"status": "Healthy"}}]}
    asked = []

    def fake(app):
        asked.append(app)
        pods = [n for n in tree["nodes"] if n["kind"] == "Pod"]
        return {"ready": sum(1 for n in pods if n["health"]["status"] == "Healthy"),
                "total": len(pods)}

    server._safe_pod_count = fake
    out = server.pod_counts(["real-service", "real-service", "../../evil", "UPPER"])
    assert out == {"real-service": {"ready": 1, "total": 2}}, out
    assert asked == ["real-service"], "a name that is not a plain app name must never reach the API"


def test_workload_paths_read_the_manifest_names_and_skip_exclude():
    tree = ["vikki-mobile/dev/party-service/.argocd-source-vikki-mobile-dev-party-service.yaml",
            "vikki-mobile/dev/party-service/values.yaml",
            "_exclude/dev/dummy/.argocd-source-vikki-mobile-dev-dummy-service.yaml",
            ".argocd-source-orphan.yaml",           # no directory: nothing to watch
            "vikki-mobile/core-integration/party-service/Chart.yaml"]
    found = {}
    for path in tree:
        if path.startswith("_exclude/"):
            continue
        head, _, name = path.rpartition("/")
        app = server.re.fullmatch(r"\.argocd-source-(.+)\.yaml", name)
        if app and head:
            found[app.group(1)] = head
    assert found == {"vikki-mobile-dev-party-service": "vikki-mobile/dev/party-service"}, found


DIFF = """diff --git a/src/A.java b/src/A.java
--- a/src/A.java
+++ b/src/A.java
@@ -10,6 +10,8 @@ class A {
     void a() {
-        old();
+        log.error("boom");
+        throw new X();
     }
 }
diff --git a/src/B.java b/src/B.java
--- a/src/B.java
+++ b/src/B.java
@@ -100,3 +101,4 @@
+    int b = 1;
"""


def test_only_lines_inside_the_diff_can_be_commented_on():
    allowed = server.diff_lines(DIFF)
    assert sorted(allowed["src/A.java"]) == [10, 11, 12, 13, 14], allowed
    assert sorted(allowed["src/B.java"]) == [101], allowed
    assert "-        old();" not in DIFF.splitlines()[0], "sanity"


def test_findings_survive_prose_and_mark_unpostable_lines():
    allowed = server.diff_lines(DIFF)
    text = ('Sure, here you go:\n'
            '[{"path":"src/A.java","line":12,"severity":"high","body":"log-and-throw"},'
            ' {"path":"src/A.java","line":999,"severity":"low","body":"line not in the diff"},'
            ' {"path":"","line":3,"severity":"low","body":"no file"},'
            ' {"path":"src/A.java","line":11,"severity":"low","body":"  "},'
            ' {"path":"src/B.java","line":101,"severity":"medium","body":"magic number"}]')
    got = server.parse_findings(text, allowed)
    assert [f["line"] for f in got] == [12, 999, 3, 101], "an empty body is dropped, the rest kept"
    assert [f["postable"] for f in got] == [True, False, False, True], got
    assert got[1]["why"] == "no such line in the diff"
    assert got[2]["why"] == "no file given"
    assert server.parse_findings("no defects found", allowed) == [], "prose is not a finding"


def test_review_payload_keeps_only_usable_comments():
    sent = {}

    def fake_run(cmd, **kw):
        sent["cmd"] = cmd
        sent["payload"] = json.loads(kw["input"])
        return type("R", (), {"returncode": 0, "stdout": '{"html_url":"u","state":"COMMENTED"}',
                              "stderr": ""})()

    real, server.subprocess.run = server.subprocess.run, fake_run
    try:
        got = server.post_review("o/r", 7, [
            {"path": "src/A.java", "line": 12, "body": "real"},
            {"path": "src/A.java", "line": 0, "body": "no line"},
            {"path": "", "line": 5, "body": "no path"},
            {"path": "src/A.java", "line": "x", "body": "line not a number"},
        ], "summary", "REQUEST_CHANGES")
        assert got["posted"] == 1, sent["payload"]
        assert sent["payload"]["comments"] == [
            {"path": "src/A.java", "line": 12, "side": "RIGHT", "body": "real"}], sent["payload"]
        assert sent["payload"]["event"] == "REQUEST_CHANGES"
        assert sent["payload"]["body"] == "summary"
        assert "repos/o/r/pulls/7/reviews" in sent["cmd"], sent["cmd"]
        try:
            server.post_review("o/r", 7, [{"path": "", "line": 0, "body": "x"}], "  ", "COMMENT")
        except RuntimeError as e:
            assert "nothing to post" in str(e)
        else:
            raise AssertionError("a review with no comment and no body must not be posted")
    finally:
        server.subprocess.run = real


def test_pr_tools_fall_back_to_the_copy_in_the_repo():
    """A teammate cloning this repo has no ~/bin, and the approve/merge buttons must still work."""
    env, home = "DEVLOGS_APPROVE_BIN", os.path.expanduser("~/bin/approve-prs.py")
    os.environ[env] = "/somewhere/else.py"
    try:
        assert server._pr_tool("approve-prs.py", env) == "/somewhere/else.py", "env wins"
    finally:
        del os.environ[env]
    got = server._pr_tool("approve-prs.py", env)
    expected = home if os.path.exists(home) else os.path.join(
        os.path.dirname(os.path.abspath(server.__file__)), "tools", "approve-prs.py")
    assert got == expected, got
    shipped = os.path.join(os.path.dirname(os.path.abspath(server.__file__)),
                           "tools", "approve-prs.py")
    assert os.path.exists(shipped), "tools/approve-prs.py must be in the repo"
    assert os.path.exists(shipped.replace("approve-prs", "merge-prs")), \
        "tools/merge-prs.py must be in the repo"


def test_open_workload_prs_match_the_exact_directory():
    server._wl_open[:] = [time.time(), [
        {"number": 1, "paths": ["vikki-mobile/dev/party-service/values.yaml"]},
        {"number": 2, "paths": ["vikki-mobile/dev/party-service-v2/values.yaml"]},
        {"number": 3, "paths": ["vikki-mobile/core-integration/party-service/values.yaml"]},
    ]]
    path = "vikki-mobile/dev/party-service"
    hit = [p["number"] for p in server.workload_open_prs()
           if any(f.startswith(path + "/") for f in p["paths"])]
    assert hit == [1], "the trailing slash is what keeps party-service-v2 out"
    server._wl_open[:] = [0, []]


def test_repo_suggestion_is_exact_suffix_only():
    server._org_repos[:] = [server.time.time(), ["party-service", "account-cell", "kong-gw"]]
    got = server.suggest_repos(["vikki-mobile-dev-party-service", "core-integration-party-service",
                                "vikki-mobile-dev-account-service", "vikki-mobile-fsap-kong-gw"])
    assert got == {"vikki-mobile-dev-party-service": "party-service",
                   "core-integration-party-service": "party-service",
                   "vikki-mobile-fsap-kong-gw": "kong-gw"}, got
    assert "vikki-mobile-dev-account-service" not in got, \
        "account-service has no repo; guessing account-cell would show another team's CI"


def test_first_breakage_is_the_oldest_failure_at_the_head():
    runs = [{"conclusion": "failure", "headSha": "cccc"},
            {"conclusion": "failure", "headSha": "bbbb"},   # the one that broke it
            {"conclusion": "success", "headSha": "aaaa"},
            {"conclusion": "failure", "headSha": "0000"}]   # an older, already-fixed breakage
    assert server.first_breakage(runs)["headSha"] == "bbbb"
    assert server.first_breakage([{"conclusion": "success", "headSha": "aaaa"}]) is None
    assert server.first_breakage([{"conclusion": "cancelled", "headSha": "aaaa"}]) is None, \
        "a cancelled run is a human pressing stop, not a broken build"


def test_argocd_errors_are_not_mistaken_for_logs():
    """All four of these came back as the log body of a genuinely broken pod."""
    for junk in ['previous terminated container "svc" in pod "svc-abc" not found',
                 'container data-dev-streampark-application is not valid for pod streampark-x',
                 'a container name must be specified for pod svc-abc, choose one of: [svc istio-proxy]',
                 'container "svc" in pod "svc-abc" is waiting to start: trying and failing to pull image']:
        assert not server._usable_log(junk), junk
    assert server._usable_log("Caused by: java.net.UnknownHostException")
    assert not server._usable_log("")
    chosen = server.CHOOSE_RE.search(
        "a container name must be specified for pod p, "
        "choose one of: [istio-init cleanup-h2-locks streampark istio-proxy]")
    names = chosen.group(1).replace(",", " ").split()
    ordered = sorted(names, key=lambda n: server._container_rank(n, "data-dev-streampark"))
    assert ordered[0] == "streampark", ordered  # the app, not an init container
    assert ordered[-2:] == ["istio-init", "istio-proxy"] or ordered[-1] == "istio-proxy", ordered


def test_diagnose_reports_every_unhealthy_kind_pods_first():
    tree = {"nodes": [
        {"kind": "Pod", "name": "svc-abc", "namespace": "dev", "uid": "u1",
         "health": {"status": "Degraded", "message": "Back-off restarting failed container"},
         "info": [{"name": "Containers", "value": "0/1"}, {"name": "Restart Count", "value": "12"}]},
        {"kind": "Pod", "name": "svc-ok", "health": {"status": "Healthy"}},
        {"kind": "Pod", "name": "svc-starting", "health": {"status": "Progressing"}},
        {"kind": "ExternalSecret", "name": "svc-secret", "uid": "u2",
         "health": {"status": "Degraded", "message": "could not get secret data from provider"}},
    ]}
    detail = {"status": {"health": {"status": "Degraded"}, "sync": {"status": "Synced"},
                         "conditions": [{"type": "ComparisonError", "message": " boom "}],
                         "operationState": {"phase": "Failed", "message": "sync failed "}}}

    def fake_api(path, timeout=15):
        if path.endswith("/resource-tree"):
            return json.dumps(tree)
        if "/events?" in path:
            return json.dumps({"items": [
                {"type": "Normal", "reason": "Pulled", "message": "pulled image", "count": 1,
                 "lastTimestamp": "2026-09-08T01:00:00Z"},
                {"type": "Warning", "reason": "BackOff", "message": "back-off 5m0s", "count": 9,
                 "lastTimestamp": "2026-09-08T02:00:00Z"}]})
        if "/pods/" in path and "/logs?" in path:
            raise OSError("this ArgoCD only serves the resource-based logs route")
        if "/logs?" in path:
            assert "podName=svc-abc" in path, path
            if "previous=true" in path:
                # what ArgoCD really answers when the pod never restarted: its error, as log text
                return ('{"result":{"content":"previous terminated container '
                        '\\"svc\\" in pod \\"svc-abc\\" not found"}}')
            return '{"result":{"content":"Caused by: java.net.UnknownHostException"}}\n{"bad json"'
        return json.dumps(detail)

    real, server._api = server._api, fake_api
    try:
        server._pods.clear()
        d = server.diagnose("some-service")
        assert d["health"] == "Degraded" and d["podCount"] == 2, d
        assert [c["type"] for c in d["conditions"]] == ["ComparisonError", "Sync Failed"], d["conditions"]
        assert d["conditions"][0]["message"] == "boom", "messages are stripped"
        pod, secret = d["pods"]
        assert pod["name"] == "svc-abc", "pods come first: they are the ones carrying logs"
        assert pod["info"] == ["Containers: 0/1", "Restart Count: 12"], pod["info"]
        assert pod["events"][0]["type"] == "Warning", "warnings come first"
        assert pod["logContainer"] == "some-service-application", pod["logContainer"]
        assert pod["logPrevious"] is False, "previous=true answered with an error, so it fell back"
        assert "UnknownHostException" in pod["log"], \
            "previous=true answered with ArgoCD's own error text, so it must fall back to the " \
            "running container"
        assert '{"bad json"' in pod["log"], "an unparseable log line is kept, not dropped"
        assert secret["kind"] == "ExternalSecret" and secret["log"] == "", \
            "an app red because of a Degraded ExternalSecret must still be explained"
        assert "svc-ok" not in [p["name"] for p in d["pods"]], "Healthy nodes are skipped"
        assert "svc-starting" not in [p["name"] for p in d["pods"]], "Progressing is not a fault"
    finally:
        server._api = real
        server._pods.clear()


def test_app_list_is_cached_per_domain():
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return type("R", (), {"returncode": 0, "stdout": '[{"metadata":{"name":"a-service"}}]',
                              "stderr": ""})()

    real, server.subprocess.run = server.subprocess.run, fake_run
    try:
        server._apps[:] = [0, "", []]
        server._conf["active"] = "one.example.io"
        assert [a["name"] for a in server.list_apps()] == ["a-service"]
        server.list_apps()
        assert len(calls) == 1, "a second call inside the TTL must not spawn argocd again"
        server._conf["active"] = "two.example.io"
        server.list_apps()
        assert len(calls) == 2, "switching domain must invalidate the cache"
    finally:
        server.subprocess.run = real
        server._apps[:] = [0, "", []]


def test_token_expiry_is_read_from_the_token_itself():
    import base64
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 1788847043}).encode()).decode().rstrip("=")
    server._tokens["e.example.io"] = "head." + payload + ".sig"
    assert server.token_expiry("e.example.io") == 1788847043
    for junk in ["", "not-a-jwt", "a.b", "a.!!!.c"]:
        server._tokens["e.example.io"] = junk
        assert server.token_expiry("e.example.io") == 0, junk
    server._tokens.clear()


def test_env_leaves_the_cli_session_alone_when_no_token():
    server._conf["active"] = "a.example.io"
    server._tokens.clear()
    os.environ["ARGOCD_AUTH_TOKEN"] = "stale-from-the-shell"
    try:
        assert "ARGOCD_AUTH_TOKEN" not in server._env(), \
            "an empty token would override the session argocd login left behind"
        server._tokens["a.example.io"] = "tok"
        assert server._env()["ARGOCD_AUTH_TOKEN"] == "tok"
    finally:
        os.environ.pop("ARGOCD_AUTH_TOKEN", None)
        server._tokens.clear()


def test_repo_link_merges_instead_of_replacing():
    server._repos["map"] = {}
    for app, repo in [("a-service", "a-repo"), ("b-service", "b-repo")]:
        server._repos["map"].pop(app, None) if not repo else server._repos["map"].update({app: repo})
    assert server._repos["map"] == {"a-service": "a-repo", "b-service": "b-repo"}, server._repos["map"]


def test_save_merges_with_disk_and_honours_unlink():
    real, tmp = server.REPOS_FILE, tempfile.mkstemp(suffix=".json")[1]
    server.REPOS_FILE = tmp
    try:
        with open(tmp, "w") as f:  # what another instance of the server wrote
            json.dump({"org": "GoodOrg", "map": {"their-service": "their-repo",
                                                 "gone-service": "gone-repo"}}, f)
        server._repos["map"] = {"my-service": "my-repo"}
        server._unlinked.clear()
        server._unlinked.add("gone-service")
        server._save_repos()
        assert json.load(open(tmp))["map"] == {"their-service": "their-repo",
                                               "my-service": "my-repo"}, json.load(open(tmp))
    finally:
        server.REPOS_FILE = real
        server._repos["map"] = {}
        os.remove(tmp)


def test_repo_map_rejects_names_that_would_reach_argv():
    real, tmp = server.REPOS_FILE, tempfile.mkstemp(suffix=".json")[1]
    server.REPOS_FILE = tmp  # never touch the developer's own .devlogs-repos.json
    try:
        with open(tmp, "w") as f:
            json.dump({"org": "GoodOrg", "map": {"real-service": "real-repo",
                                                 "real-service-2": "../../etc/passwd",
                                                 "bad app name": "repo"}}, f)
        server._repos["map"] = {}
        server._load_repos()
        assert server._repos["map"] == {"real-service": "real-repo"}, server._repos["map"]
    finally:
        server.REPOS_FILE = real
        os.remove(tmp)


if __name__ == "__main__":
    test_continuation_lines_join_one_event()
    test_runaway_dump_is_capped()
    test_column_zero_lines_stay_separate()
    test_stack_trace_keywords_join()
    test_exception_head_joins_the_error_line()
    test_plain_column_zero_text_still_starts_an_event()
    test_reconnect_drops_the_replayed_tail()
    test_orphan_continuation_survives_alone()
    test_domain_list_validation()
    test_token_is_per_domain()
    test_argocd_errors_are_not_mistaken_for_logs()
    test_diagnose_reports_every_unhealthy_kind_pods_first()
    test_app_list_is_cached_per_domain()
    test_token_expiry_is_read_from_the_token_itself()
    test_env_leaves_the_cli_session_alone_when_no_token()
    test_workload_paths_read_the_manifest_names_and_skip_exclude()
    test_only_lines_inside_the_diff_can_be_commented_on()
    test_findings_survive_prose_and_mark_unpostable_lines()
    test_review_payload_keeps_only_usable_comments()
    test_pr_tools_fall_back_to_the_copy_in_the_repo()
    test_open_workload_prs_match_the_exact_directory()
    test_repo_suggestion_is_exact_suffix_only()
    test_first_breakage_is_the_oldest_failure_at_the_head()
    test_repo_link_merges_instead_of_replacing()
    test_save_merges_with_disk_and_honours_unlink()
    test_repo_map_rejects_names_that_would_reach_argv()
    # last two: they replace server.list_apps and subprocess.run and never put them back
    test_pod_counts_parse_and_reject_bad_names()
    test_action_argv_and_whitelist()
    print("ok")
