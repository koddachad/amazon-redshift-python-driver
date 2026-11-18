"""
Live check for KAN-101: how does Redshift actually send VARBYTE values?

For each client protocol version, and for each format code the driver can ask
for in the Bind message (text=0, binary=1), this script creates a temp table
with a VARBYTE column, inserts a non-UTF-8 value and an ASCII value, and prints:

  * the `format` field the server put in the RowDescription for that column
  * the exact raw bytes that arrived in the DataRow for each value

It never decodes the value, so it cannot fail with a UnicodeDecodeError.

Connection details come from REDSHIFT_HOST / REDSHIFT_PORT / REDSHIFT_DATABASE /
REDSHIFT_USER / REDSHIFT_PASSWORD, like test_streaming_smoke.py.

Run:
    python test/manual/check_varbyte_format.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

import redshift_connector  # noqa: E402
from redshift_connector.config import FC_BINARY, FC_TEXT  # noqa: E402
from redshift_connector.utils.oids import RedshiftOID  # noqa: E402

VALUES = {"non-utf8 ff00fe": "ff00fe", "ascii 414243": "414243", "empty": ""}


def _conn_kwargs() -> dict:
    env = os.environ
    if not env.get("REDSHIFT_HOST"):
        sys.exit("Set REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD (and optionally REDSHIFT_PORT).")
    return {
        "host": env["REDSHIFT_HOST"],
        "port": int(env.get("REDSHIFT_PORT", "5439")),
        "database": env["REDSHIFT_DATABASE"],
        "user": env["REDSHIFT_USER"],
        "password": env["REDSHIFT_PASSWORD"],
        "timeout": 60,
    }


def raw_recv(data: bytes, offset: int, length: int) -> bytes:
    """Return the wire bytes untouched so we can see exactly what the server sent."""
    return bytes(data[offset : offset + length])


def run(protocol_version: int, requested_fc: int) -> None:
    kwargs = _conn_kwargs()
    kwargs["client_protocol_version"] = protocol_version
    conn = redshift_connector.connect(**kwargs)
    # Ask the server for this format code for VARBYTE and capture raw bytes.
    conn.redshift_types[RedshiftOID.VARBYTE] = (requested_fc, raw_recv)

    label = "protocol=%d (%s)  requested_format=%s" % (
        protocol_version,
        {0: "BASE_SERVER", 1: "EXTENDED_RESULT_METADATA", 2: "BINARY"}[protocol_version],
        "binary" if requested_fc == FC_BINARY else "text",
    )
    print("\n=== " + label + " ===")
    negotiated = [v for k, v in conn.parameter_statuses if k == b"server_protocol_version"]
    print("server_protocol_version reported by server:", negotiated[0].decode() if negotiated else "(none)")

    with conn.cursor() as cur:
        cur.execute("create temp table _kan101 (k varchar(32), v varbyte(64))")
        for name, hexval in VALUES.items():
            cur.execute("insert into _kan101 values (%s, from_hex(%s))", (name, hexval))
        cur.execute("select k, v from _kan101 order by k")
        desc = cur.ps["row_desc"][1]
        print("RowDescription for v: type_oid=%s format=%s (0=text, 1=binary)" % (desc["type_oid"], desc["format"]))
        for k, v in cur.fetchall():
            expected = bytes.fromhex(VALUES[k])
            if v == expected:
                verdict = "RAW BYTES"
            elif v == VALUES[k].encode():
                verdict = "HEX TEXT"
            else:
                verdict = "OTHER"
            print("  %-16s wire=%-14r  -> %s" % (k, v, verdict))
    conn.rollback()
    conn.close()


def run_default_decoders() -> bool:
    """
    Use the driver exactly as an application would (no decoder override) on
    both protocols and check every value comes back as the expected bytes.
    """
    ok = True
    for protocol_version in (2, 1):
        kwargs = _conn_kwargs()
        kwargs["client_protocol_version"] = protocol_version
        conn = redshift_connector.connect(**kwargs)
        print("\n=== protocol=%d  driver default decoders ===" % protocol_version)
        with conn.cursor() as cur:
            cur.execute("create temp table _kan101 (k varchar(32), v varbyte(64))")
            for name, hexval in VALUES.items():
                cur.execute("insert into _kan101 values (%s, from_hex(%s))", (name, hexval))
            cur.execute("select k, v from _kan101 order by k")
            for k, v in cur.fetchall():
                expected = bytes.fromhex(VALUES[k])
                good = isinstance(v, bytes) and v == expected
                ok = ok and good
                print("  %-16s value=%-16r type=%-6s -> %s" % (k, v, type(v).__name__, "OK" if good else "WRONG"))
        conn.rollback()
        conn.close()
    return ok


def main() -> int:
    for protocol_version in (2, 1):
        for fc in (FC_TEXT, FC_BINARY):
            try:
                run(protocol_version, fc)
            except Exception as e:  # noqa: BLE001
                print("  ERROR:", repr(e))
    try:
        all_ok = run_default_decoders()
    except Exception as e:  # noqa: BLE001
        print("  ERROR:", repr(e))
        all_ok = False
    print("\nDefault decoders: " + ("PASS, VARBYTE returns bytes on both protocols" if all_ok else "FAIL"))
    print("\nInterpretation: if the default (protocol=2, requested text) row shows RAW BYTES, the ticket's")
    print("diagnosis is confirmed: the server ignores the requested text format for VARBYTE on the binary")
    print("protocol and the driver must register a raw-bytes decoder for it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
