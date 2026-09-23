"""One-shot demo helper: sends exactly one SALE and one RESTOCK event for a
single product, instead of producer.py's continuous random stream across
all products. For showing a clear before/after stock change in a demo
capture (see README's "動かし方" / verify_stock_status_duckdb.sh).

Usage: python send_demo_events.py <product_id> [sale_qty] [restock_qty]
"""

import sys

from producer import TOPIC, build_producer, event


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: send_demo_events.py <product_id> [sale_qty] [restock_qty]")

    product_id = sys.argv[1]
    sale_qty = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    restock_qty = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    producer = build_producer()
    for event_type, quantity in [("SALE", sale_qty), ("RESTOCK", restock_qty)]:
        e = event(product_id, event_type, quantity)
        producer.send(TOPIC, e)
        print(e)
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
