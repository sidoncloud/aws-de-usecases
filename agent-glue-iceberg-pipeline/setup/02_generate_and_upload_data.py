"""Generate synthetic e-commerce orders and upload them to the demo bucket.

Two daily drops, shaped so an append-only pipeline gives the WRONG answer:

  dt=2026-10-13  20,000 orders, most still 'pending' or 'shipped'
  dt=2026-10-14   2,000 brand new orders
                  +800 CORRECTIONS to day-1 orders (status moved on, refunds)

The 800 corrections reuse day-1 order_ids. Append them and you double count.
MERGE on order_id is the only thing that gives you a correct current view.
"""
import csv
import io
import random
from datetime import datetime, timedelta

import boto3

random.seed(42)

REGION = "us-east-1"
ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"agent-etl-demo-{ACCOUNT}"

s3 = boto3.client("s3", region_name=REGION)

COUNTRIES = ["US", "UK", "DE", "FR", "IN", "AU", "CA", "JP"]
CHANNELS = ["web", "ios", "android", "marketplace"]
PAYMENTS = ["card", "paypal", "apple_pay", "klarna", "gift_card"]
CATEGORIES = ["Electronics", "Apparel", "Home", "Beauty", "Sports", "Toys", "Grocery"]
HEADER = ["order_id", "customer_id", "order_ts", "status", "category", "items",
          "order_total", "payment_method", "country", "channel", "updated_at"]

DAY1 = datetime(2026, 10, 13, 0, 0, 0)
DAY2 = datetime(2026, 10, 14, 0, 0, 0)


def make_order(i, base_day, status_pool):
    ts = base_day + timedelta(seconds=random.randint(0, 86399))
    items = random.randint(1, 6)
    return {
        "order_id": f"ORD_{i:07d}",
        "customer_id": f"CUST_{random.randint(1, 4000):06d}",
        "order_ts": ts.isoformat(),
        "status": random.choice(status_pool),
        "category": random.choice(CATEGORIES),
        "items": items,
        "order_total": round(items * random.uniform(8.5, 220.0), 2),
        "payment_method": random.choice(PAYMENTS),
        "country": random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "updated_at": ts.isoformat(),
    }


def to_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=HEADER)
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


def main():
    # Day 1: most orders are still in flight.
    day1 = [make_order(i, DAY1, ["pending", "pending", "shipped", "delivered"])
            for i in range(1, 20001)]

    # Day 2: new orders continue the id sequence.
    day2_new = [make_order(i, DAY2, ["pending", "pending", "shipped"])
                for i in range(20001, 22001)]

    # Day 2: corrections. Same order_id, later updated_at, status moved on.
    corrected = random.sample(day1, 800)
    day2_fixed = []
    for row in corrected:
        fixed = dict(row)
        if row["status"] == "delivered":
            fixed["status"] = "refunded"
            fixed["order_total"] = 0.0
        elif row["status"] == "shipped":
            fixed["status"] = "delivered"
        else:
            fixed["status"] = "shipped"
        fixed["updated_at"] = (DAY2 + timedelta(seconds=random.randint(0, 86399))).isoformat()
        day2_fixed.append(fixed)

    day2 = day2_new + day2_fixed
    random.shuffle(day2)

    drops = {
        "raw/orders/dt=2026-10-13/orders.csv": day1,
        "raw/orders/dt=2026-10-14/orders.csv": day2,
    }
    for key, rows in drops.items():
        s3.put_object(Bucket=BUCKET, Key=key, Body=to_csv(rows))
        print(f"  uploaded s3://{BUCKET}/{key}  ({len(rows)} rows)")

    refunds = sum(1 for r in day2_fixed if r["status"] == "refunded")
    print("\nShape of the problem:")
    print(f"  day 1 orders            {len(day1)}")
    print(f"  day 2 new orders        {len(day2_new)}")
    print(f"  day 2 corrections       {len(day2_fixed)}  (reuse day-1 order_ids)")
    print(f"    of which refunds      {refunds}")
    print(f"  correct distinct total  {len(day1) + len(day2_new)}")
    print(f"  naive append total      {len(day1) + len(day2)}  <-- wrong by {len(day2_fixed)}")


if __name__ == "__main__":
    main()
