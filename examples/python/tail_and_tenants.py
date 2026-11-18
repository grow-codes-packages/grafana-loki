"""Live tail over WebSocket and multi-tenancy with with_tenant()."""

from __future__ import annotations

import threading

import grow_grafana_loki as sdk


def main() -> None:
    with sdk.LokiClient.from_env() as client:
        team = client.with_tenant("team-a")  # sends X-Scope-OrgID (needs auth_enabled: true)
        got = threading.Event()

        def on_entries(frame: sdk.TailFrame) -> None:
            for stream in frame.streams:
                for entry in stream.entries:
                    print("tail:", stream.labels, entry.line)
            got.set()

        session = team.tail('{service_name="tail-example"}', on_entries)
        try:
            team.push([sdk.LokiStream({"service_name": "tail-example"}, [sdk.LokiEntry("hi")])])
            got.wait(10)
        finally:
            session.stop()


if __name__ == "__main__":
    main()
