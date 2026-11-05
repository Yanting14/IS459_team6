# AML Detection Pipeline

Ranks accounts for anti-money-laundering review (Q1) and measures how much more laundering banks would catch by sharing transfer data (Q2), using the synthetic IBM AML dataset.

## Language

### Parties and money movement

**Bank**:
A financial institution identified by its bank ID in the dataset.

**Account**:
A holder of money at one bank, identified by the pair (bank, account number); the account number alone never identifies an account.
_Avoid_: Customer, account number (on its own)

**Transfer**:
One movement of money from a sender account to a receiver account at a point in time.
_Avoid_: Transaction, payment

**Self-transfer**:
A transfer whose sender and receiver are the same account; it moves no money between parties.

**Counterparty**:
The account on the other side of a transfer, from the point of view of a given account.

### Views

**Shared view**:
Every transfer from every bank, as a data-sharing consortium would see it.
_Avoid_: Full view, network view

**Bank view**:
Only the transfers where one given bank is the sender's or the receiver's bank, as that bank sees them alone.
_Avoid_: Local view, own-bank view

### Detection

**Account-day**:
One account on one calendar day on which it made or received at least one transfer with another account; the unit that is scored and ranked. A day of nothing but self-transfers is not eligible. The seven-day feature table excludes self-transfers; the GIN graph snapshot and daily review score include them for eligible accounts.

**Lookback window**:
For the seven-day feature table, the 7 calendar days ending with (and including) an account-day's day. The frozen GIN uses a cumulative graph snapshot instead.

**Feature**:
A number used as model input. The seven-day table describes account behaviour over a lookback window; GIN features describe transfer edges and account nodes in a graph snapshot.

**Graph snapshot**:
Transfers from the model's time origin up to, but excluding, a declared cutoff, filtered to the shared view or bank view before computing features. Includes self-transfers and repeated transfers.

**Transfer ID**:
The source file's SHA-256 followed by its zero-based original data-row number. Stable across sorting and quarantine; separate from an account's numeric graph node ID.

**Daily review score**:
An eligible account-day's maximum incident transfer score that day, including self-transfers. Eligibility still requires at least one non-self transfer. Scores are rankings, not calibrated probabilities.

**Counterparty feature**:
A feature built from other accounts' transfers. In the seven-day table only these features differ for a bank's own accounts between views. The GIN additionally depends on the visible graph and its node summaries.

**Label**:
Ground truth, never a model input. A transfer label records whether that transfer was marked as laundering. An account-day label records whether the account took part in a laundering transfer on that day, at the eligible account-day grain.
_Avoid_: Is laundering, target

### Data quality

**Quarantined row**:
A source row rejected because the data itself is broken (a missing field, an unreadable timestamp, a non-positive amount); kept aside with its reason, never used downstream.
_Avoid_: Bad row, error row

**Missing feature value**:
A feature with no meaningful value for an account-day (e.g. hours from receiving to sending for an account that received nothing); the account-day stays in, unlike a quarantined row.

### Laundering

**Ring**:
One laundering attempt listed in the dataset's patterns file: a set of transfers forming a known shape such as a fan-out or a cycle.
_Avoid_: Pattern, scheme
