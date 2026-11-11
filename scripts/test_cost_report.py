"""Formats canned Cost Explorer GetCostAndUsage pages into the cost slide's table.

Cost Explorer is never called: each request costs US$0.01.
"""

import cost_report


def group(service, amount):
    return {"Keys": [service], "Metrics": {"UnblendedCost": {"Amount": amount, "Unit": "USD"}}}


def month(start, end, *groups):
    return {"TimePeriod": {"Start": start, "End": end}, "Groups": list(groups)}


# Two pages, as Cost Explorer splits them on NextPageToken. EMR appears in both months and
# on both pages, so its total is summed across all of them.
PAGES = [
    {
        "ResultsByTime": [
            month("2026-10-01", "2026-11-01",
                  group("Amazon Elastic MapReduce", "10.50"),
                  group("Amazon Simple Storage Service", "0.40"),
                  group("AWS Key Management Service", "0")),
        ],
        "NextPageToken": "page-2",
    },
    {
        "ResultsByTime": [
            month("2026-11-01", "2026-11-11",
                  group("Amazon Elastic MapReduce", "2.00"),
                  group("Amazon Athena", "1.10"),
                  group("AWS Glue", "0.004")),
        ],
    },
]


def test_services_are_summed_over_months_and_pages_and_sorted_by_cost():
    lines = cost_report.format_report(PAGES).splitlines()

    assert lines[:5] == [
        "| Service | US$ |",
        "|---|---:|",
        "| Amazon Elastic MapReduce | 12.50 |",
        "| Amazon Athena | 1.10 |",
        "| Amazon Simple Storage Service | 0.40 |",
    ]


def test_services_under_one_cent_are_folded_into_other_then_the_total_and_budget_share():
    lines = cost_report.format_report(PAGES).splitlines()

    # KMS (0) and Glue (0.004) fold into "other"; 14.004 is 5.6% of 250.
    assert lines[5:] == [
        "| other | 0.00 |",
        "| **Total** | **14.00** |",
        "",
        "US$14.00 is 5.6% of the US$250 budget.",
    ]


class FakeCostExplorer:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get_cost_and_usage(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages[len(self.calls) - 1]


def test_fetch_follows_next_page_token():
    client = FakeCostExplorer(PAGES)

    assert cost_report.fetch_pages(client, "2026-10-01", "2026-11-11") == PAGES
    assert "NextPageToken" not in client.calls[0]
    assert client.calls[1]["NextPageToken"] == "page-2"
    assert client.calls[0]["TimePeriod"] == {"Start": "2026-10-01", "End": "2026-11-11"}
    assert client.calls[0]["Granularity"] == "MONTHLY"
    assert client.calls[0]["Metrics"] == ["UnblendedCost"]
    assert client.calls[0]["GroupBy"] == [{"Type": "DIMENSION", "Key": "SERVICE"}]
