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
One account on one calendar day on which it made or received at least one transfer; the unit that is scored and ranked.

**Lookback window**:
The 7 calendar days ending with (and including) an account-day's day.

**Feature**:
A number describing an account's behaviour over its lookback window, used as model input.

**Counterparty feature**:
A feature built from other accounts' transfers, so it is the only kind whose value differs between the shared view and a bank view.

**Label**:
Whether an account took part in a laundering transfer on that account-day; the training target and never a model input.
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
