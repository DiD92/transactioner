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

The binary uses the `csv` crate to deserialize the file into a `Vec` of `Transaction` entries. Paired with those entries the binary also generates a `HashMap` of `ClientAccount` entries, which house the transaction processing logic.

The code doesn't use `unsafe` at any point, and all code is meant to run in `stable`

The application in its current state is fully serial, everything runs on the main thread.

### Testing

Some testing cases have been provided, along with them some test files are available in the `test_data` folder. The bigger files are only used as reference in performance measurements.

### Error handling

Most errors are not properly handled, the only errors the code 'kinda' handles are those related to the transaction processing logic, any other errors
such as problems opening the file or malformed data are not handled at all.

### Efficiency

The most notable optimization is done in the `TransactionType` enum, which is internally represented as an `u8` compared to the much larger size it would have been to store the transaction type as an `String`.

Although not implemented, it could be possible to load the file in chunks, but since a 1.000.000 record CSV barely takes 30 MB, it has not been considered worth it.


### Maintanabilty

The code is all located in `src/main.rs` which hurts its ease to read and maintain, this has been done in order to speed up development time taking into account time available to develop the code.
