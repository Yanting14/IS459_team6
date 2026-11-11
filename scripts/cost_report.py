"""Prints spend per AWS service since the project started, as a Markdown table for the cost slide.

    python scripts/cost_report.py [--start 2026-10-01] [--end <today>]

Notes:
- Each Cost Explorer request costs US$0.01; a run makes one request per page.
- Cost Explorer data lags by roughly a day, so today's spend is not in the figure yet.
- The figure is account-wide, not just this project: cost allocation tags only count
  from the day they are activated.
- --end is exclusive, as in Cost Explorer.
"""

import argparse
import datetime
from collections import defaultdict
from decimal import Decimal

BUDGET = Decimal("250")
FOLD_BELOW = Decimal("0.01")


def fetch_pages(client, start, end):
    """Every GetCostAndUsage page for [start, end), following NextPageToken."""
    kwargs = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "MONTHLY",
        "Metrics": ["UnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    pages = []
    while True:
        page = client.get_cost_and_usage(**kwargs)
        pages.append(page)
        if not page.get("NextPageToken"):
            return pages
        kwargs["NextPageToken"] = page["NextPageToken"]


def format_report(pages):
    """The Markdown table of US$ per service, summed over every month on every page."""
    costs = defaultdict(Decimal)
    for page in pages:
        for result in page["ResultsByTime"]:
            for group in result["Groups"]:
                costs[group["Keys"][0]] += Decimal(group["Metrics"]["UnblendedCost"]["Amount"])

    total = sum(costs.values(), Decimal(0))
    # abs: a large credit or refund is negative but must still be shown.
    shown = sorted(((s, c) for s, c in costs.items() if abs(c) >= FOLD_BELOW), key=lambda sc: -sc[1])
    lines = ["| Service | US$ |", "|---|---:|"]
    lines += [f"| {service} | {cost:.2f} |" for service, cost in shown]
    if len(shown) < len(costs):
        other = sum((c for c in costs.values() if abs(c) < FOLD_BELOW), Decimal(0))
        lines.append(f"| other | {other:.2f} |")
    lines += [
        f"| **Total** | **{total:.2f}** |",
        "",
        f"US${total:.2f} is {total / BUDGET:.1%} of the US${BUDGET} budget.",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default=datetime.date.today().isoformat())
    args = parser.parse_args()

    import boto3  # only here, so the tests need no AWS SDK or credentials

    print(format_report(fetch_pages(boto3.client("ce"), args.start, args.end)))


if __name__ == "__main__":
    main()
