# transactioner

This small binary takes a transactions file and outputs the final state of each client involved in the transactions.

## Input format

The binary takes a single argument as input, which is the path of the CSV file to process, the CSV file has the following format:

```csv
type,       client,  tx, amount
deposit,         1,   1,    1.0
deposit,         2,   2,    2.0
deposit,         1,   3,    2.0
withdrawal,      1,   4,    1.5
withdrawal,      2,   5,    3.0
```

## Implementation

### Basics

The binary uses the `csv` crate to deserialize the file into a `Vec` of `Transaction` entries.

The code doesn't use `unsafe` at any point, and all code is meant to run in `stable`

The application is parallelized in the processing stage using `tokio` workers with a threaded runtime. The transactions are sent to the corresponding workers based on the client identifier, this is done in order to avoid
the usage of shared data between the worker threads as much as possible.

### Testing

Some testing cases have been provided, along with them some test files are available in the `test_data` folder. The bigger files found in the`perf` sub-folder are only used as reference in performance measurements.

### Error handling

Most errors are not properly handled, the only errors the code 'kinda' handles are those related to the transaction processing logic, any other errors
such as problems opening the file or malformed data are not handled at all.

### Efficiency

The most notable optimization is done in the `TransactionType` enum, which is internally represented as an `u8` compared to the much larger size it would have been to store the transaction type as an `String`.

In order to attain a higher speed in the hashing rate, the code uses the `twox-hash` crate which implements the `XxHash` algorithm, which gives up being cryptographically secure
in order to obtain higher hashing rates, which in this application is considered a priority.

The transactions are also processed as soon as they are read, so we basically read and process the file concurrently, which allows for some speedups compared to the original serial version.

### Maintainability

The code is all located in `src/main.rs` which hurts its ease to read and maintain, this has been done in order to speed up development time, a more production-ready version would have some split around type definitions and runtime management.

### Limitations

The main limitation of the application is the speed at which we can read the file, because of that, adding more worker threads is generally not worth it unless the time it takes for the system to parse
the file improves. Even though, reaching this limitation means that in terms of cpu time, the application is already doing its best.

A sample result processing a 1 million transaction file with an 8 core `AMD Ryzen 3500U` gives the following approximate values:

```bash
time target/release/transactioner test_data/perf/1_000_000.csv > out.csv
Using 2 worker thread/s to process "test_data/perf/1_000_000.csv" using a channel buffer size of 120000 Bytes

real    0m0,834s
user    0m1,440s
sys     0m0,426s
```

Which results in approximately **~1.200.000** transactions per second. Or conversely **~600.000** transactions per second per core. As stated earlier, 
increasing the number of workers (even though we have more cores cpu cores available) doesn't really provide any speedup as seen below:

```bash
Using 3 worker thread/s to process "test_data/perf/1_000_000.csv" using a channel buffer size of 120000 Bytes

real    0m0,931s
user    0m1,840s
sys     0m0,803s
```

In the case of only 1 worker we see how the performance has degraded a bit, but not much.

```bash
time target/release/transactioner test_data/perf/1_000_000.csv > out.csv
Using 1 worker thread/s to process "test_data/perf/1_000_000.csv" using a channel buffer size of 240000 Bytes

real    0m1,090s
user    0m1,290s
sys     0m0,067s
```

In the end, it seems that the optimal number of cores for the machine in which the tests have been performed seems to be 2,
of course different hardware configurations may result in a different optimal number, but the file can be read in parallel, we 
won't really get much in terms of speedups than the current implementation.
