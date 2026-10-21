# Background schema reconciliation and active writers

Background type reconciliation runs each migration attempt in an independent
transaction. It uses the existing per-table application migration lock with a
zero acquisition timeout, then reserves the existing PostgreSQL table with
`LOCK TABLE ... IN ACCESS EXCLUSIVE MODE NOWAIT` before changing metadata or DDL.
The same background transaction caps PostgreSQL `lock_timeout` at 1ms (without
loosening a pre-existing stricter value), so semantic-only registry writes also
yield to occupied metadata rows. In-memory storage needs neither reservation.

An occupied application lock or PostgreSQL table lock returns `BUSY`. The attempt
rolls back before the result reaches the queue. The queue defers the DAO to the
next tick without storing a successful model fingerprint, so the model change is
retried even if no new registry event arrives. Deferred busy DAOs follow the
unprocessed remainder, preserving the existing per-tick table limit. Other
migration errors retain their existing failure reporting and are not retried as
contention. The timeout is transaction-local; commit/rollback restores the pooled
connection's prior setting. Foreground transactions do not receive this cap.

This avoids an application/database lock inversion observed with data 1.74.0:

1. A record transaction holds a PostgreSQL table lock after writing a record.
2. Background reconciliation takes the application migration lock and attempts
   `ALTER TABLE`, waiting for that record transaction to finish.
3. A nested mutation in the record transaction needs the application migration
   lock. PostgreSQL cannot detect this cross-system wait cycle; the foreground
   operation eventually fails at the configured migration-lock timeout.

The foreground mutation path retains its configured timeout (10 seconds by
default), transactional DDL and rollback semantics. Its application lock still
covers the migration callback rather than the entire record transaction; this
change does not redesign concurrent foreground migrations or make all lock
acquisition/network operations latency-bounded. Background work may wait for a
quieter tick; an ordinary mutation can still migrate columns it requires.

Verification uses PostgreSQL, an explicitly held ROW EXCLUSIVE lock, a separate
reconciler thread, a nested foreground mutation, an occupied application lock,
a locked metadata row during TEXT-to-OPTIONS relabeling, physical connection reuse,
and a queue retry with no additional model event. The conversion retry checks
that the original text value remains in its backup column. The previously
published implementation fails the active-writer test; the corrected candidate
passes it. Release/delivery status is recorded in the operational task
`ecos-data-schema-lock-inversion-20261021`; this document does not claim a rollout.
