#!/usr/bin/env python3
"""Self-check for the line-joining logic: python3 test_server.py (no framework, no network)."""
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
    test_action_argv_and_whitelist()  # last two: they monkeypatch module globals
    test_pod_counts_parse_and_reject_bad_names()
    print("ok")
