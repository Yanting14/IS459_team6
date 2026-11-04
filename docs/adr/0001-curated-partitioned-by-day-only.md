# Curated zone is partitioned by day only, sorted by bank

The proposal says curated data is "split by bank and day", but with thousands of banks and 16 days that produces tens of thousands of tiny Parquet files, which slows every Spark and Athena read. We partition curated tables by `date` only and sort rows by `bank_id` within each file, so a single bank's view is still cheap to read through Parquet min/max statistics.
