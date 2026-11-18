"""
Manual smoke test for the fork's streaming cursor against a real cluster.

Connection details come from environment variables (preferred, nothing lands
on disk) or, as a fallback, from the [ci-cluster] / [default-test] sections of
test/config.ini:

    REDSHIFT_HOST, REDSHIFT_PORT (default 5439), REDSHIFT_DATABASE,
    REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SSLMODE (optional)

Run with:

    python test/manual/test_streaming_smoke.py [--table test_rt.salesorderdetails2]

What it checks:
  1. A streaming cursor reads rows lazily: peak RSS stays flat while
     iterating the full table.
  2. Closing a streaming cursor early reconnects and the connection is
     immediately usable for a new query.
  3. Executing a new statement while a stream is open reconnects cleanly.
  4. A non-streaming cursor on the same connection still works afterwards
     (prepared statement cache fix from upstream 2.1.13).
  5. fetchmany() with streaming_batch_size > 1 returns the right counts.
  6. The new verify-full default connects (and logs, rather than fails, on
     a hostname mismatch).
"""
import argparse
import configparser
import logging
import os
import resource
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

import redshift_connector  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")


def _rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _conn_kwargs() -> dict:
    env = os.environ
    if env.get("REDSHIFT_HOST"):
        kwargs = {
            "host": env["REDSHIFT_HOST"],
            "port": int(env.get("REDSHIFT_PORT", "5439")),
            "database": env["REDSHIFT_DATABASE"],
            "user": env["REDSHIFT_USER"],
            "password": env["REDSHIFT_PASSWORD"],
        }
        if env.get("REDSHIFT_SSLMODE"):
            kwargs["sslmode"] = env["REDSHIFT_SSLMODE"]
        return kwargs

    conf = configparser.ConfigParser()
    ini = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.ini")
    if not conf.read(ini):
        sys.exit("No REDSHIFT_* env vars and no test/config.ini found; see module docstring.")
    kwargs = {
        "host": conf.get("ci-cluster", "host"),
        "port": conf.getint("default-test", "port", fallback=5439),
        "database": conf.get("ci-cluster", "database"),
        "user": conf.get("ci-cluster", "test_user"),
        "password": conf.get("ci-cluster", "test_password"),
    }
    if conf.has_option("default-test", "sslmode"):
        kwargs["sslmode"] = conf.get("default-test", "sslmode")
    return kwargs


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(("PASS" if ok else "FAIL") + "  " + name + ("  (" + detail + ")" if detail else ""), flush=True)
    return ok


def step(msg: str) -> None:
    print("   ... " + msg, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="test_rt.salesorderdetails2")
    parser.add_argument("--max-rows", type=int, default=0, help="stop full scan after N rows (0 = whole table)")
    parser.add_argument("--debug", action="store_true", help="log every protocol message the driver sends/receives")
    parser.add_argument("--timeout", type=int, default=120, help="socket timeout in seconds so a hang fails instead of blocking")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger("redshift_connector").setLevel(logging.DEBUG)

    kwargs = _conn_kwargs()
    kwargs["timeout"] = args.timeout
    sql = "select * from " + args.table
    results = []

    # ---- 6. connect with the new verify-full default -------------------------
    t0 = time.time()
    conn = redshift_connector.connect(**kwargs)
    results.append(check("connect (sslmode default verify-full)", True, "%.2fs" % (time.time() - t0)))

    # Use the catalog estimate rather than count(*): a full count on a very large
    # table can take minutes and tells us nothing about the driver.
    schema, _, table = args.table.rpartition(".")
    step("looking up row estimate in svv_table_info")
    with conn.cursor() as cur:
        cur.execute(
            "select tbl_rows from svv_table_info where \"schema\" = %s and \"table\" = %s",
            (schema or "public", table),
        )
        row = cur.fetchone()
    total = int(row[0]) if row else 0
    print("table rows (catalog estimate): %d" % total, flush=True)
    if total == 0:
        print("could not find the table in svv_table_info; row-count checks will be skipped", flush=True)

    # ---- 1. full streaming scan, memory stays flat ---------------------------
    rss_before = _rss_mb()
    cur = conn.cursor()
    cur.streaming = True
    cur.streaming_batch_size = 1000
    step("executing streaming query (returns after the first DataRow)")
    t0 = time.time()
    cur.execute(sql)
    step("execute returned in %.2fs; iterating" % (time.time() - t0))
    rss_after_execute = _rss_mb()
    n = 0
    first_row = None
    last_report = time.time()
    for row in cur:
        if first_row is None:
            first_row = row
        n += 1
        if time.time() - last_report > 5:
            step("%d rows so far, RSS %.0f MB" % (n, _rss_mb()))
            last_report = time.time()
        if args.max_rows and n >= args.max_rows:
            break
    elapsed = time.time() - t0
    rss_after_scan = _rss_mb()
    step("closing streaming cursor")
    cur.close()
    expected = args.max_rows if args.max_rows else total
    # svv_table_info is an estimate, so accept the exact count or within 1 % of it.
    count_ok = n == expected or (not args.max_rows and total and abs(n - total) <= max(1, total // 100))
    results.append(check("streaming scan row count", count_ok, "%d rows in %.1fs, %.0f rows/s" % (n, elapsed, n / elapsed if elapsed else 0)))
    results.append(
        check(
            "streaming scan memory flat",
            rss_after_scan - rss_after_execute < 100,
            "RSS before %.0f MB, after execute %.0f MB, after scan %.0f MB" % (rss_before, rss_after_execute, rss_after_scan),
        )
    )
    results.append(check("description populated", cur.description is not None and len(cur.description) == len(first_row), str(cur.description[0]) if cur.description else "none"))

    # ---- 2. early close reconnects, connection reusable ----------------------
    step("early-close test: executing streaming query")
    cur = conn.cursor()
    cur.streaming = True
    cur.execute(sql)
    rows = [next(cur) for _ in range(5)]
    step("closing after 5 rows (triggers reconnect)")
    t0 = time.time()
    cur.close()
    reconnect_time = time.time() - t0
    with conn.cursor() as cur2:
        cur2.execute("select 1")
        ok = cur2.fetchone()[0] == 1
    results.append(check("early close reconnects and connection reusable", ok and len(rows) == 5, "close took %.2fs" % reconnect_time))

    # ---- 3. new execute while stream open ------------------------------------
    step("execute-while-streaming test")
    cur = conn.cursor()
    cur.streaming = True
    cur.execute(sql)
    next(cur)
    cur3 = conn.cursor()
    cur3.execute("select 2")
    results.append(check("execute while streaming reconnects", cur3.fetchone()[0] == 2))
    cur3.close()
    cur.close()

    # ---- 4. non-streaming cursor still fine, prepared statement cache ---------
    step("non-streaming / prepared statement cache test")
    ok = True
    try:
        with conn.cursor() as cur4:
            for i in range(3):
                cur4.execute("select %s", (i,))
                ok = ok and cur4.fetchone()[0] == i
            cur4.execute("create temp table _smoke_tmp (a int)")
            cur4.execute("select %s", (99,))
            ok = ok and cur4.fetchone()[0] == 99
            conn.rollback()
            cur4.execute("select %s", (7,))
            ok = ok and cur4.fetchone()[0] == 7
    except Exception as e:  # noqa: BLE001
        ok = False
        print("   error:", repr(e))
    results.append(check("non-streaming queries after DDL/ROLLBACK", ok))

    # ---- 5. fetchmany with batch size ----------------------------------------
    step("fetchmany test")
    cur = conn.cursor()
    cur.streaming = True
    cur.streaming_batch_size = 250
    cur.execute(sql)
    a = cur.fetchmany(250)
    b = cur.fetchmany(250)
    cur.close()
    results.append(check("fetchmany with streaming_batch_size", len(a) == min(250, total) and len(b) == min(250, max(0, total - 250)), "%d + %d rows" % (len(a), len(b))))

    conn.close()
    print("\n%d/%d checks passed" % (sum(results), len(results)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
