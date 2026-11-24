# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The adapter dials the control plane with mutual TLS: its client certificate, its key and the
CA it trusts, each from a file. Without all three it refuses to start, unless --insecure says
so explicitly, which is for local tests only. There is no insecure default. The EdgeStream is
dialled the same way."""

from __future__ import annotations

import pytest

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot


@pytest.fixture
def tls_files(tmp_path):
    files = {}
    for name in ("ca.crt", "tls.crt", "tls.key"):
        p = tmp_path / name
        p.write_bytes(f"-----{name}-----".encode())
        files[name] = p
    return files


class _Channel:
    """Enough of a channel for the generated stub to be built on; it carries nothing."""

    def __getattr__(self, name):
        return lambda *a, **kw: None


@pytest.fixture
def dialled(monkeypatch):
    """Record every channel the adapter would open; open none, start no stream."""
    calls = []
    monkeypatch.setattr(adapter_mod.grpc, "insecure_channel",
                        lambda target, **kw: calls.append(("insecure", target, kw)) or _Channel())
    monkeypatch.setattr(adapter_mod.grpc, "secure_channel",
                        lambda target, creds, options=None: calls.append(
                            ("secure", target, creds, list(options or []))) or _Channel())
    monkeypatch.setattr(adapter_mod.grpc, "ssl_channel_credentials",
                        lambda **kw: ("creds", kw))
    monkeypatch.setattr(Adapter, "run_safety_stream", lambda self, stub: None)
    monkeypatch.setattr(Adapter, "run_control_stream", lambda self, stub: None)
    return calls


def _main(*argv):
    adapter_mod.main(["--endpoint", "cp.example:9090", *argv])


def _error(capsys) -> str:
    """argparse's own error line: the usage line above it names every flag, so asserting on all
    of stderr would pass whichever refusal ran."""
    (line,) = [x for x in capsys.readouterr().err.splitlines() if ": error: " in x]
    return line


def test_no_tls_files_and_no_insecure_flag_refuses_to_start(dialled, capsys):
    with pytest.raises(SystemExit) as exc:
        _main()
    assert exc.value.code == 2
    err = _error(capsys)
    assert "mutual TLS needs" in err
    assert "missing: --tls-ca, --tls-cert, --tls-key" in err and "--insecure" in err
    assert dialled == []


@pytest.mark.parametrize("missing", ["--tls-ca", "--tls-cert", "--tls-key"])
def test_a_missing_tls_file_flag_refuses_to_start_and_is_named(dialled, tls_files, capsys, missing):
    given = {"--tls-ca": tls_files["ca.crt"], "--tls-cert": tls_files["tls.crt"],
             "--tls-key": tls_files["tls.key"]}
    given.pop(missing)
    with pytest.raises(SystemExit):
        _main(*[str(x) for kv in given.items() for x in kv])
    assert f"(missing: {missing})" in _error(capsys)
    assert dialled == []


def test_an_unreadable_tls_file_refuses_to_start(dialled, tls_files, tmp_path, capsys):
    with pytest.raises(SystemExit):
        _main("--tls-ca", str(tmp_path / "absent.crt"), "--tls-cert", str(tls_files["tls.crt"]),
              "--tls-key", str(tls_files["tls.key"]))
    err = _error(capsys)
    assert err.split(": error: ")[1].startswith("--tls-ca ") and "absent.crt" in err
    assert dialled == []


def test_insecure_together_with_tls_files_refuses_to_start(dialled, tls_files, capsys):
    with pytest.raises(SystemExit):
        _main("--insecure", "--tls-ca", str(tls_files["ca.crt"]), "--tls-cert",
              str(tls_files["tls.crt"]), "--tls-key", str(tls_files["tls.key"]))
    assert "--insecure and the --tls-* flags together" in _error(capsys)
    assert dialled == []


def test_tls_files_dial_mutual_tls_with_their_bytes(dialled, tls_files):
    _main("--tls-ca", str(tls_files["ca.crt"]), "--tls-cert", str(tls_files["tls.crt"]),
          "--tls-key", str(tls_files["tls.key"]), "--tls-server-name", "controlstream.svc")
    ((kind, target, creds, options),) = dialled
    assert (kind, target) == ("secure", "cp.example:9090")
    assert creds == ("creds", {"root_certificates": b"-----ca.crt-----",
                               "private_key": b"-----tls.key-----",
                               "certificate_chain": b"-----tls.crt-----"})
    assert options == [("grpc.ssl_target_name_override", "controlstream.svc")]


def test_insecure_is_explicit_and_says_so(dialled, capsys):
    _main("--insecure")
    assert [c[:2] for c in dialled] == [("insecure", "cp.example:9090")]
    assert "INSECURE" in capsys.readouterr().err


def test_the_edge_stream_is_dialled_with_the_same_credentials() -> None:
    opened = []
    robot = SimulatedRobot()
    robot.spawn("amr-1")
    a = Adapter(pb, None, robot, "amr-1", dial=lambda target: opened.append(target) or object())
    a._dial_edge("edge-1:7000")
    assert opened == ["edge-1:7000"]


def test_the_edge_loop_dials_through_the_adapters_dialler() -> None:
    opened, stubs = [], []

    class _PbGrpc:
        class EdgeServiceStub:
            def __init__(self, channel):
                stubs.append(channel)

            def EdgeStream(self, requests):
                a._shutdown.set()  # so the ended stream isn't dialled again, and the loop returns
                return iter(())

    robot = SimulatedRobot()
    robot.spawn("amr-1")
    channel = object()
    a = Adapter(pb, _PbGrpc, robot, "amr-1", dial=lambda target: opened.append(target) or channel)
    a._process._start_edge = a._process._edge_loop  # inline
    a._process.edge_endpoints(a, ["edge-1:7000"])
    assert opened == ["edge-1:7000"] and stubs == [channel]
