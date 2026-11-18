"""Push streams (JSON and protobuf), then read them back with query_range, labels and series."""

from __future__ import annotations

import time
import uuid

import grow_grafana_loki as sdk
from grow_grafana_loki import LogQL


def main() -> None:
    run = uuid.uuid4().hex[:8]
    service = f"example-{run}"
    with sdk.LokiClient.from_env() as client:
        now = time.time_ns()
        client.push(
            [
                sdk.LokiStream(
                    # "run" is not in the cardinality allowlist: the SDK sends it as structured
                    # metadata instead of an index label.
                    {"service_name": service, "level": "info", "run": run},
                    [
                        sdk.LokiEntry("user signed in", now, {"user_id": "42"}),
                        sdk.LokiEntry('{"msg": "slow request", "status": 503}', now + 1),
                    ],
                )
            ]
        )
        client.push(
            [sdk.LokiStream({"service_name": service}, [sdk.LokiEntry("via protobuf")])],
            format="protobuf",
        )

        query = LogQL.selector({"service_name": service}).json().where("status", ">=", 500)
        print("LogQL:", client.format_query(str(query)))
        for stream in client.query_range(str(query), start=now, direction="forward").streams:
            for entry in stream.entries:
                print(stream.labels, entry.line, entry.parsed)

        metric = client.query(f'sum(count_over_time({{service_name="{service}"}}[5m]))')
        print("lines in the last 5 minutes:", metric.vector[0].value.value)
        print("labels:", client.labels())
        print("levels:", client.label_values("level", query=f'{{service_name="{service}"}}'))
        print("series:", client.series([f'{{service_name="{service}"}}']))
        print("volume:", client.volume(f'{{service_name="{service}"}}').vector)
        print("server:", client.build_info().version)


if __name__ == "__main__":
    main()
