# Operation result authority binding

Status: implemented; portable result/replay qualification remains open.

## Invariant

An operation result is valid only when its normalized `operation_hash` equals
the authoritative operation hash read under the same Postgres transaction.
The relational `result_id` and embedded result contract must also represent
the same stored result.

## Implementation

`OperationStore.RecordResult` now locks and reads the authoritative operation
before accepting a result, rejects a hash mismatch, and writes a generated
result identity back into the JSON contract before persistence. The result
stable hash continues to exclude delivery identity, so retries remain
idempotent while the durable record no longer contains an empty embedded ID.

## Cost and licensing

This adds no query beyond the operation row lock already required by the
atomic result transition. Invalid submissions fail before lease or result
mutation. The implementation uses the existing Fornix MIT-licensed store and
does not copy reference code.

## Acceptance tests

- a result for another operation hash is rejected;
- valid results transition exactly once;
- duplicate result delivery returns the committed record without a live lease;
- generated result identity is present in both relational and JSON records;
- workspace, task-fence, and replay checks remain green.
