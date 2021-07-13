# Tiny Transaction Processor

Tiny transaction processor is a small command-line program that processess a CSV file with
a list of transactions and outputs the resulting state of the accounts. Supported types of transactions
are deposits and withdrawals. Those can be disputed with a follow-up settlement of the dispute
with either a resolution or a chargeback.

Checkout and run:

```
git clone https://github.com/alexandervoronov/tiny-transaction-processor.git
cd tiny-transaction-processor
cargo run --release -- {path-to-transaction-file}
```

An example CSV input:

```
type,       client, tx, amount
deposit,        23,  1,     10
chargeback,     23,  1
deposit,        24,  2,     15
deposit,        42,  3,     12.5
withdrawal,     22,  4,      7
withdrawal,     42,  5,      2.25
deposit,        23,  6,      8
withdrawal,     23,  7,      2
dispute,        23,  1
deposit,        24,  8,     16
dispute,        42,  5
chargeback,     42,  5
dispute,        24,  2
resolve,        23,  1
deposit,        42,  9,      6.5
withdrawal,     24, 10,      3.2
```

The outcome of these transactions will be the following:

```
client,available,held,total,locked
42,8,0,8,true
24,12.8,15,27.8,false
23,16,0,16,false
```

## Usage

```
tiny-transaction-processor {path-to-transaction-file}
```

Tiny transaction processor takes a single argument, which is the path to the CSV file with the list of
transactions. The output of the client account state is printed to `stdout` and all the errors
are logged into `stderr`.
Transactions that failed to parse or that can't be processed are ignored but don't stop the processing of
the remaining transactions.

### Logging verbosity

Log level is controlled via environment variable `RUST_LOG`. The default log level is `info` as
the tiny transaction processor doesn't log much. All the errors related to CSV parsing or
transaction processing are logged at the `error` level.

To adjust the log level you can prefix the command with environment variable, for example

```
RUST_LOG=error ./tiny-transaction-processor transactions.csv
```

To disable all the logging use `RUST_LOG=off`.

## Implementation details

### Type system

As there are two major classes of transactions, _Transaction_ is implemented as a `enum` of either 
_Transfer_ (for deposits and withdrawals)
or _Amendment_ (for disputes, resolutions and chargebacks). I'm sure there are better terms for these two classes,
but being not a native speaker in English and not having background in finance, these are the best I was able to
come up with.

I chose [Decimal](https://crates.io/crates/rust_decimal) for money representation, which should keep us away from
potential rounding errors and seems to provide capacity for up to 10^27, which is a good upper limit. If we need
to represent larger amounts, could do a wider search for similar libraries or implement something in case of very
specific requirements.

The format correctness checks are mostly carried by [serde](https://crates.io/crates/serde) and
[csv](https://crates.io/crates/csv). The only additional checks needed at the construction point are for a _Transfer_ to have an amount present and it being non-negative.

### Errors

There are two main error types. _InputFormatError_ covers CSV parsing errors and the issues of missing or invalid
amount for _Transfers_. _ProcessingError_ covers exception cases supported by the tiny transaction
processor like trying two withdraw more than available mount or duplicated chargeback of the same transaction.
There is no pretty formatting for the errors yet the names of the errors are intended to be descriptive and
sufficient for analysis and development purposes. The error types could be improved to implement
_Error_ trait and provide the conventional information expected from errors.

### Processing assumptions and additional rules

- The second dispute of a transaction that is already in dispute is ignored. This prevents from unnecessarily
  doubling the held amount. If the dispute has been resolved, the transaction can be disputed again.
  Disputes of a charged back transaction are also ignored.
- _Transfer_ that reuses a transaction ID of one of the previous _Transfers_ is ignored.
- If an _Amendment_ has a client ID that doesn't match the one in the disputed transaction, it's ignored.
- If an _Amendment_ has an amount specified, it still counts as a valid transaction, yet the amount value
  is ignored.
- _Disputes_ of both _Deposits_ and _Withdrawals_ will reduce the available balance. It feels unintuitive for
  _Withdrawals_, and if we had double-entry transactions, probably only the credited account should have been
  affected. With the given format I assumed that the money were questionably spent and as there might be somebody
  in the world that would like to be compensated, we hold the given amount until further information. Which also
  seems to be in line with the specification.
- When account is locked after a _Chargeback_, all the further _Deposits_ and _Withdrawals_ are ignored. There is
  no way to unlock a locked account. _Disputes_ and further _Chargebacks_ of other transactions are allowed.
- It a _Transfer_ was ignored, it also can't be disputed. The error will be reported as unfamiliar transaction.

The described assumptions are covered by tests. As a trade-off towards conciseness/readability of the tests,
I didn't test for exact for exact errors being reported. In a production system would in order to have more
confidence that the test covers the intended code path. All the tests for both CSV parsing and transaction
processing are in _tests_ folder and can be run with `cargo test`.

### Thoughts on performance and scaling

While I don't have any actual profiling results, I can share some thoughts on further maintenance/scaling
of the tiny transaction processor.

The code is organised in a way that input and transaction processing are independent, so they also can be profiled
and tuned independently. For the purpose of such profiling, it will be nice to have a snapshot of real-world
data to understand the number of transaction for one day/week, ratio between _Transfers_ and _Amendments_, and
the nature and the frequency of client account balance queries.

If we use the tiny transaction processor in online mode and need higher throughput, we can quite naturally
scale it by having multiple instances of the tiny transaction processor (separate threads, processes or
even independent machines) and shard between them by client ID.

The main concern for high- hroughput is the number of _Transfers_ we need to store for potential disputes.
The amount representation by the _Decimal_ takes 16 bytes. The other fields + hashmap overhead will probably bring
us to ≈64 bytes per record, which means 64 GB machine will be able to accommodate about 1 billion transactions.
Unless we have convenient dispute-time deadlines, we may need to look into storing transactions in a database.
