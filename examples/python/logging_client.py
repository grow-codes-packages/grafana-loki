"""Log records through the batching LogClient, then search them (category interface)."""

from __future__ import annotations

import logging
import uuid

import grow_grafana_loki as sdk


def main() -> None:
    run = uuid.uuid4().hex[:8]
    with sdk.LokiClient.from_env() as client:
        log_client = client.log_client(
            resource={"service.name": "checkout", "deployment.environment": "dev"}
        )
        logger = log_client.logger("orders").child({"run": run})
        logger.info("order placed", {"order_id": "o-1", "password": "never-sent"})  # redacted
        logger.error("payment failed", {"order_id": "o-1"})

        # stdlib logging goes to Loki too
        logging.getLogger("app").addHandler(sdk.LogHandler(log_client))
        logging.getLogger("app").warning("from the logging module %s", run)
        log_client.flush(10)

        query = client.log_query()
        record = query.wait_for_log(f'{{service_name="checkout"}} | run="{run}"', timeout_s=30)
        print(record.severity.name, record.body, record.attributes)
        page = query.search("payment failed", limit=10)
        for item in page.records:
            print(item.timestamp, item.body)
        print("streams:", query.list_streams()[:5])
    # client.close() flushed and closed the log client


if __name__ == "__main__":
    main()
