# Architecture

How the code is organised, why the seams are where they are, and what to know
before changing it. For operating the tool see `README.md`.

Module `holdfast`, Go 1.24. Dependencies: `pgx/v5`, `aws-sdk-go-v2` (s3store),
`golang.org/x/crypto`, `yaml.v3`.

---

## 1. Package layout and dependency direction

```
main.go                              flags, config loading, wiring (driver, encryptor, store), dispatch, exit codes
types/                               vocabulary. imports nothing internal.
  backup_type.go                     Medium, Level, BackupType
  backup.go                          BackupRequest, ParentRef, DumpOutcome, BackupResult
  meta.go                            BackupMeta sidecar, ExtractedBackup
  config.go                          Config + Validate
  cli.go                             CLIOptions, RunMode, ValidateForMode
  cli_results.go                     result structs for the non-backup modes
  storage.go                         ObjectInfo, ErrObjectNotExist, ValidateObjectName
interfaces/                          verbs.
  backup_db.go                       BackupDB — the engine driver contract
  encryptor.go                       Encryptor
  storer.go                          Storer — backup_dir behind an interface (see §10)
  filestore/                         Storer over a directory
  s3store/                           Storer over an S3 bucket
  mirrorstore/                       Storer over a primary plus mirrors
  storetest/                         the contract both must pass
  postgres/                          the only BackupDB implementation
    postgress_backupdb.go            preflight, logical dump, dispatchers
    capabilities.go                  SupportedTypes / Supports / ValidateRequest
    basebackup.go                    physical full + incremental
    validate_chain.go                pg_combinebackup + container boot
  crypto/aes_encryptor.go            the only Encryptor implementation
api/                                 orchestration. knows interfaces, never drivers or stores.
  backup.go                          StartBackup, writeEncryptedArchive, fallbackToFull
  parent.go                          SelectParentMeta, MaterialiseParentState — chain walking
  lock.go                            AcquireRunLock — one run per host (local disk)
  diskspace.go                       free-space checks (scratch, local disk)
  meta.go                            sidecar read/write, MetaScan
  state_cache.go                     parent-manifest cache (local disk)
  retention.go                       chain-safe cleanup (see §7)
  util.go                            object names, ObjectLocation, decryptObject
  validate_only.go, decrypt.go, dry_run.go, archive.go, sweep.go
```

`main → api → interfaces → types`. Nothing points back up. `api` never imports
`interfaces/postgres`, `interfaces/filestore` or `interfaces/s3store`; its
tests import the stores the way they import the fake driver.

**The rule that keeps this working:** `types` holds nouns, `interfaces` holds
verbs, and neither holds engine-specific knowledge.

---

## 2. The two-layer validation split

This is the most important design decision in the codebase and the easiest to
accidentally undo.

`types.BackupType` is a `(Medium, Level)` pair, not a closed enum:

```go
type BackupType struct {
    Medium Medium  // "logical" | "physical"
    Level  Level   // "full" | "incremental"
}
```

`types` validates only that each axis is a recognised word. It deliberately
**accepts `logical-incremental`**, which Postgres cannot produce. Two tests
assert that acceptance explicitly.

Which combinations are real is a driver question, answered by
`BackupDB.Supports` and `BackupDB.ValidateRequest`. Postgres rejects
`logical-incremental` there, because `pg_dump` cannot emit a delta. MySQL's
binlog replay makes the same combination perfectly valid — putting the rule in
`types` would bake one engine's limitation into the package every engine must
import.

The same reasoning shapes the field names: `ParentRef.StatePath` and
`DumpOutcome.State` are opaque driver artifacts (a `backup_manifest` for
Postgres, a GTID set elsewhere), and `PositionStart`/`PositionEnd` are the
engine's own notation. `api` passes these around and never opens them.

**If you find yourself adding a Postgres-specific rule to `types`, it belongs in
`capabilities.go` instead.**

---

## 3. The backup pipeline

```
main → api.StartBackup(ctx, cfg, store, db, encryptor, …)
  ├─ api.AcquireRunLock                     flock in state_dir, held all run
  │    contention → SkipError (exit 0); anything else → hard error
  ├─ api.SweepStaleTemps                    store.Sweep + scratch leftovers, under the lock
  ├─ api.SelectParentMeta                   sidecars only (store.List/Open) → ParentRef
  │    └─ IncrementalUnavailable → fallbackToFull
  ├─ backupDB.ValidateRequest(req)          engine rules
  ├─ backupDB.ShouldRun(ctx, req)           preflight; false = SkipError, exit 0
  ├─ api.MaterialiseParentState             EnsureState; cold cache → decryptObject; failure → fallbackToFull
  ├─ writeEncryptedArchive                  the type-agnostic core
  │    ├─ store.Stat × 2                    collision refusal (Commit would replace)
  │    ├─ w := store.Create(archive)        invisible until Commit
  │    ├─ goroutine: zip.Writer → io.Pipe
  │    │    └─ backupDB.DumpToArchive(ctx, zw, req) → DumpOutcome
  │    ├─ encryptor.EncryptStream(pipe → w)
  │    ├─ w.Commit()                        durable; Abort on every error path
  │    ├─ PutObject(.sha256)                failure deletes the archive again
  │    ├─ IncrementalUnavailable → fallbackToFull, retry once
  │    └─ returns artifact + outcome
  ├─ api.WriteState                         cache the manifest (local) for the next incremental
  ├─ api.WriteMeta                          sidecar → store
  └─ if enable_restore_test:
       ├─ api.ExtractChain                  decryptObject + unzip lineage, oldest first
       ├─ backupDB.ValidateChain(chain)
       └─ WriteMeta again with the result
```

`writeEncryptedArchive` is type-agnostic — physical backups needed no changes to
it, because a `base.tar` is just another zip entry. It is also store-agnostic:
the encryptor writes into whatever `store.Create` returned. The `io.Pipe`
means the plaintext archive never exists on disk as a whole file, and for S3
the ciphertext never does either.

**The `done` channel is load-bearing.** `DumpToArchive` returns a value read
after `EncryptStream` finishes; without waiting for the producer goroutine that
read races the write. The original code got away with it only because the dump
returned nothing but an error.

Retention is *not* called from `StartBackup`. `main` runs it as a separate step
through `api.RunRetention`, which takes the run lock itself. It runs whether or
not the backup step succeeded: only a skip or a cancellation bypasses it.

Backup IDs are UTC timestamps. `writeEncryptedArchive` refuses to proceed if an
archive or sidecar with the same ID already exists.

`api.decryptObject` is the one read path for archives: `store.Open` gives a
reader and a size, `Encryptor.DecryptStream` consumes exactly that many bytes,
the plaintext lands in `<out>.tmp` and is renamed only once the MAC verifies.
A SHA-256 of the ciphertext is computed on the same pass, so `-decrypt` of a
bucket-held archive reads it once, not twice.

---

## 4. Chains and the sidecar

Every backup writes `backup_<id>.meta.json` next to its archive. A chain is
reconstructible from the backup directory alone — no database, no external
index, no passphrase — because that directory may be all that survives.

```
ChainID   the root full's ID. A full has ChainID == ID.
ParentID  the immediate predecessor, empty for a full.
```

`BackupMeta.Validate()` enforces the invariants that make chain-walking safe: a
full has no parent and is its own chain root; an incremental has a parent and a
different chain root. `WriteMeta` refuses to write anything that fails.

**Sidecars are signed.** `WriteMeta` takes a key from `api.MetaKey(passphrase)`
(scrypt over the passphrase with a fixed salt, memoised) and stores an
HMAC-SHA256 over the identity fields in `signature`. `ReadMeta` with a key
refuses a sidecar whose signature does not verify; `ReadAllMeta` puts it in
`Skipped`, which stops retention. A sidecar with no signature at all is, by
default, accepted as predating signing and counted in `MetaScan.Unsigned`;
`StartBackup` signs every such sidecar it finds (`SignUnsignedSidecars`), so
one run per host is the migration. With `require_signed_sidecars: true`
(`MetaPolicy.RequireSigned`) an unsigned sidecar is `Skipped` like a tampered
one, and a scan with no key refuses outright (`ErrNoMetaKey`) rather than
trusting everything — a sync host without the passphrase can copy but not
prune. Until that flag is on, the signature only catches accidents: an
attacker who can edit a sidecar can also drop the field. A nil key disables
both signing and verification, which is what tests use and what a
`-retention-only` run without a passphrase gets (with a warning).

**The sidecar is unencrypted, so it must never contain schema information.**
There is no `Entries` field for exactly this reason: for a logical backup the
zip entries are `<dbname>.dump` for every database, which would disclose the
whole inventory to anyone who can `ls`. `EntryCount` replaces it, and
`TestMetaDoesNotLeakEntryNames` asserts it stays that way.

`api.ReadAllMeta` returns `MetaScan{Backups, Skipped}` and never fails on a
corrupt file. That split matters: a backup run may proceed with skipped
sidecars, but anything that **deletes** must refuse — an unreadable sidecar
describes a backup whose dependents are unknown.

### Parent resolution lives in `api`, not the driver

`SelectParentMeta` reads sidecars, picks the newest chain tip **of the same
medium**, and refuses three things: a tip that has not passed a restore test
(chaining onto an unproven backup compounds a problem instead of catching it),
a chain already at `max_chain_length`, and a lineage with any member whose
sidecar or archive is missing from disk. `MaterialiseParentState` then makes
the parent's `backup_manifest` available, and reports failure as
`IncrementalUnavailableError` too: a parent that will not decrypt is a reason
to take a full, not to exit 1 forever.

The medium filter exists because a directory commonly holds two schedules. With
a logical job running daily, the newest backup overall is usually logical — and
rooting a physical incremental on it fails deep inside `pg_basebackup`.

Chain walking is a repository question, not an engine one. Putting it in the
driver means every future backend reimplements it, differently.

---

## 5. Encryption format

`crypto.AESEncryptor`, `Name() == "aes-ctr-hmac"`. scrypt (N=2^15, r=8, p=1)
derives 64 bytes → 32-byte AES key + 32-byte MAC key.

```
"DBBK1"  5B   magic
salt     16B  random per file
iv       16B  random per file
ciphertext    AES-256-CTR
tag      32B  HMAC-SHA256 over magic ‖ salt ‖ iv ‖ ciphertext
```

Encrypt-then-MAC. `EncryptStream` tees output through a separate SHA-256 so the
`.sha256` sidecar covers the finished ciphertext. `Decrypt` writes to
`<out>.tmp` and renames only after the MAC verifies, so a wrong passphrase never
leaves a partial plaintext.

No algorithm-agility field beyond the magic string: a format change means
bumping `DBBK1` and branching on it.

---

## 6. Postgres driver

**`ShouldRun`** — checks `pg_is_in_recovery()` against `run_on_cluster_role`;
for replicas, that the WAL receiver is `streaming` and replay lag is within
`max_replica_lag`. For incrementals it additionally verifies server version 17+,
`summarize_wal = on`, and WAL summary coverage.

**`dumpLogicalFull`** — `pg_dumpall --globals-only` streams straight into the
zip; each database is dumped with `pg_dump -Fc` to a scratch file, 3 at a time,
then copied in sequentially. Auth via a temp `.pgpass` and `PGPASSFILE`, so the
password never appears in a process argument list.

**`dumpPhysical`** — `pg_basebackup -Ft -X stream --checkpoint=fast` into
scratch, then `base.tar`, tablespace tars, `pg_wal.tar`, `backup_manifest` into
the zip in that order. The manifest is returned as `DumpOutcome.State` and
parsed for its WAL range.

**`validatePhysicalChain`** — untars each member back into a backup directory,
runs `pg_combinebackup`, then boots a container on the result. The container
runs as the calling uid with the data dir at `0700`, because Postgres refuses to
start on a directory it does not own or that is group-readable. The `chmod 0777`
used by the logical validation path would actively break this.

The restored cluster is booted with this tool's overrides on the `postgres`
command line, never with the production configuration it carries:
`archive_mode=off`, empty `restore_command`/`primary_conninfo`/
`synchronous_standby_names`, `shared_preload_libraries=`, `ssl=off`,
`shared_buffers=128MB`, `huge_pages=off`, and a trust-only `pg_hba.conf` written
into the data directory. The sanity query connects as the role and database
from `pg_conn_string`, which are the ones known to exist in the restored
cluster. Preflight refuses a physical backup of a cluster with user tablespaces
when restore tests are on, because this path cannot rebuild them.

### The `IncrementalUnavailable` path

`types.IncrementalUnavailableError` means "an incremental cannot be taken right
now, but a full would work" — aged-out WAL summaries, or a standby with no new
restartpoint. `StartBackup` catches it and, with `fallback_to_full: true`,
retries as a full. A missed incremental leaves the run with no backup at all,
which is worse than an unexpectedly large one.

---

## 7. Known gaps and defects

**Retention is chain-safe but is not a policy engine.** `CleanupRetention`
groups by `ChainID`, deletes a chain only once its *newest* member has aged
out, deletes whole chains rather than members, removes the archive, `.sha256`
and `.meta.json` together, refuses to run at all while any sidecar fails to
parse, and never deletes the newest `min_chains_kept` chains (default 1).
That removes the two data-loss hazards — a full can no longer be pruned out
from under its incrementals, and a run of failed nights can no longer age out
the last good chain — but GFS is still not implemented: there is no "keep 12
monthlies", and no tiering.

The intended shape for that, still not built: a **pure** `Plan(backups, policy,
now) (*RetentionPlan, error)` with no filesystem, clock, or config access, plus
`Apply(plan)` doing the I/O. Purity makes DST boundaries and clock skew
table-testable. Classify GFS tiers at expiry time rather than tagging at
creation — tag-at-creation breaks whenever a scheduled run is missed.
`docs/retention-handoff.md` is the full brief: the current algorithm, the nine
invariants that must survive, the policy shape, the test matrix and the open
decisions. Start there.

Remaining gaps, in impact order:

1. **Peak scratch usage is still large.** A chain validation needs roughly
   N+2 cluster-sized copies for an N-member chain: each member's unpacked tree
   plus `pg_combinebackup`'s output. Tars are now released as soon as they are
   unpacked, and the combine step checks free space first, but nothing reduces
   the fundamental requirement. `max_chain_length` is effectively a disk-space
   setting, and validating the whole chain on *every* incremental is the thing
   to reconsider first — validating on the full and every Nth incremental would
   cut this sharply.
2. **Cross-host runs are not serialised.** `AcquireRunLock` is an flock in
   `state_dir`, which is local, so it protects against overlapping cron on one
   host — the failure that actually happens. Two hosts writing to one shared
   `backup_dir` need a lease in something that can arbitrate; nothing models
   that yet.
3. **`maxParallelDumps = 3`** is a hardcoded const.
4. **Tablespaces unsupported** in physical validation (fails loudly).
5. **Validation proves the cluster boots, not that the delta applied.**
   `bootAndCheck` asserts the restored cluster starts and holds at least one
   non-template database. A `pg_combinebackup` that silently dropped changes
   would pass. The highest-value addition to this codebase is a verification
   query against known post-parent data.
6. **No metrics output.** Exit 0 covers both success and a healthy skip, so
   monitoring has to parse stdout JSON. A `--metrics-file` in node_exporter
   textfile format would be a small, high-value addition.

### Defects fixed, and the invariant each one protects

Fourth pass (October 2026):

- **Retention could delete the last backup.** It runs after every attempt,
  succeeded or not, and had no floor: more failed nights than
  `retention_days` left an empty directory with exit 1 every run.
  `min_chains_kept` (default 1) keeps the newest N chains whatever their age;
  the floor is positional, counted over live and expired chains alike, so
  `RetentionPolicy` and `RetentionStats.ChainsKeptByFloor` make it visible.
  `TestRetentionFloorNeverDeletesLastChain`.
- **Sidecar signing was bypassable.** An unsigned sidecar was accepted as
  pre-signing, so deleting the `signature` field made any edit trusted.
  `StartBackup` now signs legacy sidecars, `require_signed_sidecars` makes an
  unsigned one untrusted, and a strict scan without a key refuses rather than
  trusting everything. `TestRequireSignedRefusesUnsignedSidecar`,
  `TestSignUnsignedSidecars`.
- **`dump_timeout: 0` failed every physical backup.** The physical path called
  `context.WithTimeout` directly, where zero is "already expired"; the logical
  path treated zero as unlimited. Both go through `withOptionalTimeout`.
  `TestWithOptionalTimeoutZeroIsUnbounded`.

Third pass (October 2026, with the storage seam, §10):

- **A mirror failure cost the local backup, or went unnoticed.** Neither is
  acceptable: `mirrorstore` aborts the failed mirror's writer so it publishes
  nothing partial, lets the primary complete, records the failure, and `main`
  exits 3. Exit 0 would hide an offsite copy falling behind; exit 1 would
  make the local backup look lost. `TestMirrorFailureIsRecordedNotFatal`.
- **A completed multipart upload left an orphan on one provider.** Against
  Ceph RGW a 6 MiB archive uploaded, verified and was deleted, yet an
  incomplete multipart upload for the same key stayed open, billed for its
  parts and invisible to listing. `s3store.Sweep` aborts such uploads once
  they are older than `StaleUploadAge`; the S3 tests fail if any upload is
  left open after cleanup, and the README tells operators to set a bucket
  lifecycle rule as well.
- **An S3 host with no credentials waited on the instance-metadata timeout
  inside the first request.** The SDK chain ends at EC2 IMDS, which does not
  exist on an NFS server, and `sudo` drops the environment that would have
  supplied the keys. `s3store.New` now resolves credentials once at
  construction with a bounded context and names the three ways to supply
  them; a custom endpoint also disables the IMDS lookup outright.
  `TestNoCredentialsFailsFastAtConstruction`.
- **A storage-only sync config passed validation and failed every request
  for want of a region.** Compatible endpoints take any region but SigV4
  needs one in the request. With an endpoint the store defaults to
  `us-east-1`; without one (real AWS) validation requires the bucket's
  region or `AWS_REGION`, because a wrong region there fails every request
  with a redirect the SDK does not follow. `TestEndpointWithoutRegionWorks`,
  `TestStorageValidation`.
- **`-sync-mirrors -prune` could empty the bucket on an unmounted share.**
  An unmounted `backup_dir` lists as empty, and "delete what the primary no
  longer has" would then be everything. Prune refuses when the primary lists
  no backup objects at all, and when any primary sidecar is untrusted
  (retention's rule). `TestSyncPruneIsOptInAndGuarded`.
- **A corrupt local archive would have been replicated as if good.** Sync
  hashes each archive while streaming it and compares with its `.sha256`;
  a mismatch aborts that object's copy and the run reports it.
  `TestSyncRefusesToReplicateCorruptArchive`.
- **Two sources on one host could prune each other's objects.** Two backup
  directories syncing to one prefix would each delete what the other has.
  Sources carry distinct prefixes by construction (default: the name) and
  validation rejects a collision, including one produced through
  `storage.s3.prefix`. `TestSourcesValidation`.
- **`-decrypt` of a remote archive would have read it twice.** The checksum
  pass and the decrypt pass are now one read: `decryptObject` hashes the
  ciphertext on the way through and keeps the plaintext only if the MAC and
  the recorded checksum both agree.

Second pass (October 2026):

- **Any lock failure was a healthy skip.** `StartBackup` turned every error
  from `AcquireRunLock` into `SkipError`, so an uncreatable `state_dir` meant
  no backup, ever, with exit 0 every run. Contention is now `*LockHeldError`
  and the only thing that skips; a holder older than
  `dump_timeout + restore_timeout` is reported as a stuck run instead.
- **The physical restore test booted the production configuration.** See §6.
  Before, `shared_preload_libraries`, `ssl`, `huge_pages`, `shared_buffers` or
  the source `pg_hba.conf` failed the test for reasons unrelated to the
  backup, and `archive_mode=on` would have run the real `archive_command` from
  the container.
- **A failing restore test stopped retention and forced a full every run.**
  `main` skipped retention on any error; the next run saw an unvalidated tip
  and fell back to a full. Retention now runs regardless, and clusters with
  tablespaces are refused in preflight rather than after the dump.
- **Crashed runs left cluster-sized garbage forever.** `.backup_*.tmp` in
  `backup_dir` and `holdfast_*` directories in `scratch_dir` were invisible to
  retention. `SweepStaleTemps` removes them at the start of every run that
  holds the lock.
- **A parent whose archive was gone was still a parent.** With a warm state
  cache nothing opened the archive. `SelectParentMeta` now stats every archive
  in the candidate lineage; a cold cache that fails to decrypt is a fallback,
  not a permanent exit 1.
- **`isSummaryUnavailable` matched guessed strings.** The standby-restartpoint
  case the comments kept naming ("manifest requires WAL from final timeline
  ...") was not recognised. Patterns now follow the server's `errmsg` text.
- **`restore_timeout` did not apply to logical validation.** The readiness
  loop and `pg_restore` ran on the raw run context.
- **`restore_all_dbs` did nothing.** Removed, along with its env var.
- **Backup IDs were local time.** The repeated hour at the end of DST produced
  a duplicate ID and the second run overwrote the first. IDs are UTC and an
  existing archive or sidecar with the same ID aborts the run.
- **An incremental needed scratch for a whole cluster.** The preflight
  estimate ignored the level and the refusal was a hard error. Incrementals
  now need a fifth of the cluster as a floor.
- **`.pgpass` fields were not escaped.** A password containing `:` or `\`
  passed pgx preflight and failed every `pg_dump`.
- **`-dry-run` discarded its diagnosis on failure.** `main` printed only
  "config validation failed"; it now prints the result JSON before exiting 1.
- **Sidecars were unauthenticated.** See §4.
- **Retention, `-test-only` and `-retention-only` ran unlocked.** All three
  now take the run lock; `-test-only` reports contention as an error, the
  others as a skip.
- **Parent state was decrypted before the role check.** A node about to be
  told "wrong role" first decrypted a cluster-sized archive. Materialisation
  moved after `ShouldRun`.
- **`dump_timeout` was per database.** It now bounds the whole dump step.
- **`-decrypt` dropped plaintext next to the archive by default.** It now
  defaults to the current directory and refuses to overwrite.
- **Unix-socket connection strings were rejected** by `Config.Validate` and
  mis-handled in `.pgpass`.
- **Logical validation errors did not say why.** A `pg_restore` failure now
  includes the last output lines and a hint when the cause is an extension
  missing from the image.

First pass:

Recorded because each was reachable in normal operation, and the reasoning is
easy to undo by accident.

- **The pipeline deadlocked when the output write failed.** `EncryptStream`
  returns on its first failed write without draining the pipe, so the dump
  goroutine blocked in `pw.Write` forever and the `<-done` that stops the
  outcome read from racing it never returned. A full backup mount — the most
  likely failure this tool has — hung the process instead of reporting ENOSPC.
  `writeEncryptedArchive` now calls `pr.CloseWithError(err)` before waiting.
  `TestWriteEncryptedArchiveDoesNotHangOnOutputFailure` deadlocks the package
  if this is reverted.
- **An impossible incremental aborted the run instead of taking a full.**
  Parent resolution lived in `main`, so a chain at `max_chain_length`, a fresh
  host, or a tip that failed its restore test exited 2 with no backup taken —
  on every subsequent run, until a human intervened. Resolution moved into
  `StartBackup`, and every such condition is now
  `IncrementalUnavailableError`, which `fallbackToFull` downgrades. That
  function is applied at all three points where the condition can surface:
  parent selection, preflight, and the dump itself. The dump path previously
  had no fallback at all, and it is the one that actually fires, because the
  summary-coverage precheck is best-effort.
- **`enable_restore_test: false` made incrementals impossible.** Nothing sets
  `validationResult` in that mode, so every tip looked unproven and the
  validated-tip rule rejected all of them. The rule is now conditional on the
  flag.
- **Retention orphaned sidecars.** Deleting the archive but not the
  `.meta.json` left a file `ReadAllMeta` accepts as a valid backup, so parent
  resolution could select a tip whose archive was gone, and chain validation
  could walk into a deleted ancestor.
- **The durability guarantee was backwards.** `WriteMeta` fsynced the sidecar
  while the archive it describes was never flushed, so a power loss could
  leave a durable sidecar asserting a size and SHA-256 for a truncated file.
  The archive, the checksum and both directories are now fsynced.
- **Preflight ordering paged on healthy nodes.** Incremental preconditions ran
  before the cluster-role gate, so a wrong-role node returned a hard error
  (exit 1) where the same node asked for a logical backup skipped cleanly
  (exit 0). Role is checked first: whether a node should back up at all does
  not depend on the backup type.
- **`isSummaryUnavailable` matched any stderr containing "manifest"**, so a
  permissions error was classified as recoverable and silently converted every
  future incremental into a full while reporting success.
- **`-dry-run` was neither dry nor fast.** It went through `ResolveParent`,
  which decrypts the whole parent archive and writes to `state_dir`.
  `SelectParentMeta` splits sidecar reading from state materialisation so the
  dry run reports its plan without side effects.
- **Nothing recorded an out-of-band validation.** `-test-only` ran the restore
  test but never wrote the result, so a backup could not be proven restorable
  after the fact and one failure blocked every later incremental.
- **Chains could span a timeline change.** The manifest's timeline was parsed
  and discarded. It is now carried through `DumpOutcome` → `BackupMeta` →
  `ParentRef` and checked against `pg_control_checkpoint()`; a fork means a
  full, not a silently invalid chain.
- **Confused lag arithmetic** — `lag := time.Duration(8)` (8ns) plus three
  overlapping branches. A NULL replay timestamp now reports unbounded lag
  explicitly instead of relying on the branches happening to agree.
- **SQL injection** in logical validation's `CREATE DATABASE`, from a zip entry
  name, now `pgx.Identifier{}.Sanitize()`.
- **Database names became zip entries verbatim**, so a name containing `/` or
  `..` produced an archive `Unzip` refuses. `safeAsArchiveEntry` fails at dump
  time rather than at restore.
- **Version skew was undetectable until the expensive work was done.**
  `pg_basebackup`, `pg_combinebackup` and `pg_test_docker_image` are checked
  against the server's major version in preflight.
- **Other:** `max_wal_senders` preflight for `--wal-method=stream`; scratch
  free-space checks before a physical dump and before combining; `scratch_dir`
  defaulted so it cannot silently become tmpfs; `-test-only` staged in
  `scratch_dir` rather than `/tmp`; `.pgpass` created with `O_EXCL` instead of
  a predictable name plus a symlink-following `WriteFile`; malformed
  `RETENTION_DAYS`/`MAX_REPLICA_LAG_SECONDS` rejected instead of silently
  ignored; `datallowconn` filtered in the database listing; deterministic
  tablespace tar ordering; temp archive removed when finalisation fails.

---

## 8. Tests

71 tests across `types/`, `api/` and `interfaces/`. Beyond the original
coverage of type parsing, the `omitzero` stdout contract, two-layer request
validation, sidecar round-trips and the no-schema-leakage guarantee:

Storage seam (third pass):

- `interfaces/storetest` — the Storer contract, run by every backend: large
  multipart round trip, abort leaves nothing, uncommitted is invisible,
  missing objects, delete, prefix listing, replace, writer lifecycle, bad
  names, cancelled contexts. It also hosts the in-process fake S3 the other
  packages share.
- `interfaces/filestore` — the contract, plus sweep removes only uncommitted
  leftovers and listing skips dotfiles and directories.
- `interfaces/s3store` — the contract against the in-process fake by
  default and a real endpoint when `HOLDFAST_S3_TEST_ENDPOINT` is set;
  stale multipart uploads are swept and recent ones kept; a missing bucket
  fails instead of hanging; no region with an endpoint works; no
  credentials fails fast. Cleanup fails the test if any upload is left open.
- `interfaces/mirrorstore` — the contract over two stores, reads come from
  the primary only, a mirror failure is recorded and not fatal, a primary
  failure aborts the mirrors, a missing mirror object is not a delete
  failure.
- `api/e2e_test.go` — the real orchestration (full → incremental on a cold
  cache → validate-only → retention → decrypt → wrong passphrase → dry run)
  over file, S3, and file mirrored to S3, with the real encryptor and the
  fake driver.
- `api/sync_test.go` — copy everything then nothing, re-copy a same-size
  changed sidecar, refuse a corrupt archive, prune is opt-in and guarded,
  copy order, several sources are independent and aggregated.
- `types/config_test.go` — storage validation per kind, sync mode needs
  only storage, sources validation (names, directories, prefixes), source
  selection and prefix composition.

Earlier passes:

- `api/pipeline_test.go` — the output-failure deadlock, and that a failed run
  leaves no partial artefacts.
- `api/retention_chain_test.go` — a chain with a live tip survives, an expired
  chain is deleted whole including sidecars, retention refuses while a sidecar
  is unreadable, un-described archives age out, `retention_days: 0` is inert.
- `api/parent_test.go` — all four rollover conditions are recoverable, the
  validated-tip rule follows `enable_restore_test`, the medium filter, and
  lineage cycle/missing-ancestor detection.
- `api/lock_test.go`, `api/lock_errors_test.go` — mutual exclusion, verified
  against a second process because flock is per file description and a
  same-process test would pass vacuously; contention is typed, a fault is not
  contention, and a stuck holder is not a skip.
- `api/meta_sign_test.go` — signed round trip, a tampered sidecar is skipped
  and stops retention, an unsigned one is still accepted.
- `api/sweep_test.go` — leftovers go, real backups and foreign directories
  stay.
- `api/parent_disk_test.go` — a lineage with a missing archive is unavailable
  (auto) or an error (explicit `-parent`).
- `interfaces/crypto/aes_encryptor_test.go` — multi-chunk round trip, wrong
  passphrase leaves no plaintext, a flipped ciphertext bit is caught, a
  truncated file is rejected.
- `interfaces/postgres/entryname_test.go` — unsafe archive entry names, image
  tag parsing, and the recoverable/not-recoverable boundary in
  `isSummaryUnavailable`.

Still nothing that touches a real database or Docker. That gap is the reason
§7 item 5 matters: the end-to-end incremental path can only be proven against a
live cluster.

---

## 9. Adding a second engine

The seam was built for this; here is the checklist.

1. Implement `interfaces.BackupDB` in `interfaces/<engine>/`.
2. `SupportedTypes()` declares the real combinations — MySQL would include
   `logical-incremental` via binlog replay, which Postgres cannot.
3. `ValidateRequest` holds the engine's parent rules.
4. `DumpToArchive` returns `DumpOutcome.State` — whatever a later incremental
   needs as its `StatePath`.
5. Wire it in `main.go` alongside `postgres.NewPostgresBackupDB`.

Two places currently assume Postgres and would need generalising:
`api.StateEntryName` is the constant `"backup_manifest"`, and `main.go`
constructs the Postgres driver unconditionally. Both are small; neither is in
`types`.

---

## 10. Storage seam

`interfaces.Storer` abstracts **backup_dir and nothing else**: the encrypted
archive, its `.sha256`, and its `.meta.json`. `state_dir` (flock, parent
manifest cache) and `scratch_dir` (pg_basebackup staging, chain extraction)
stay local. An flock has no meaning in a bucket, and the drivers hand scratch
trees to `pg_combinebackup` by path.

```go
type Storer interface {
    Name() string; Location() string
    Create(ctx, name) (ObjectWriter, error)        // invisible until Commit
    Open(ctx, name) (io.ReadCloser, int64, error)  // size: the decryptor needs the HMAC offset
    Stat(ctx, name) (types.ObjectInfo, error)      // types.ErrObjectNotExist
    List(ctx, prefix) ([]types.ObjectInfo, error)  // committed objects only, sorted
    Delete(ctx, name) error                        // ErrObjectNotExist when nothing was there
    Sweep(ctx) error                               // leftovers of crashed writers; run-lock only
}
```

Names are flat basenames, rejected otherwise. The sidecar already records
`EncryptedFile` as a basename so a set survives being moved; a Storer that
took paths would reintroduce the coupling.

`Create` is the load-bearing method. Its writer must make nothing visible
until `Commit`, and `Commit` must mean durable. That is the "temp file, fsync,
rename, fsync dir" sequence `writeEncryptedArchive` and `WriteMeta` perform
today, and `filestore` performs exactly it. `s3store` streams through an
`io.Pipe` into a multipart upload that is completed on `Commit` and aborted on
`Abort` or failure; a failed upload closes the pipe's read side so a producer
blocked in `Write` is released, the same deadlock §3 describes. `Commit`
replaces an existing object: the archive-collision refusal stays in `api`,
and `WriteMeta` rewrites sidecars on purpose.

`interfaces/storetest` is the behavioural contract. Both backends run it;
`s3store` runs it against an in-process fake by default and against a real
endpoint when `HOLDFAST_S3_TEST_ENDPOINT` is set.

**Primary and mirrors.** `storage.primary` names the authoritative store and
`storage.mirrors` any number of others; the usual shape is `primary: file`,
`mirrors: [s3]`. `mirrorstore` fans out `Create`, `Delete` and `Sweep` and
answers `Open`, `Stat` and `List` from the primary alone, so parent selection,
chain walking and retention see one consistent view, and backup_dir behaves
exactly as it did before mirrors existed. A mirror failing is recorded, logged,
and never fails the operation: the mirror's writer is aborted so it publishes
nothing partial, the primary completes, and `main` exits 3 (not 0, not 1) when
any mirror operation failed during the run. With a mirror the dump runs at the speed of
the slowest destination.

**Sync mode.** `-sync-mirrors` (`api.SyncMirrors`) is the other way to get
an offsite copy: the database host backs up with no mirrors, and a host that
serves backup_dir — the NFS server, typically — runs the sync on a schedule
with the same backup_dir and `mirrors: [s3]`. It lists the primary, copies
what each mirror lacks or holds differently (size for large objects, content
for small ones, because a sidecar rewrite can keep its length), and verifies
every archive against its `.sha256` while streaming it, so a corrupted local
copy is reported rather than replicated. Archives and checksums go before
sidecars so a mirror never holds a sidecar without its archive. `-prune` also
deletes mirror objects the primary no longer has, with two refusals: a
primary that lists nothing (an unmounted share looks exactly like that) and a
primary with any unreadable sidecar (retention's rule). It takes this host's
run lock, needs no passphrase, prints `SyncResult` and exits 3 when any
object failed, so cron retries it. Nothing serialises it against the backup
host; a retention pass there mid-copy costs one failed object, retried next
run.

**Sources.** `types.SourceConfig` lets one config name several backup
directories, each with a mirror prefix that defaults to its name under
`storage.s3.prefix`. Validation rejects duplicate names, directories and
effective prefixes, which is what makes `-prune` safe across sources. `main`
resolves the selection: `-sync-mirrors` builds one store set per enabled
source and hands them to `api.SyncSources`, which runs them in turn under a
single run lock and returns one result per source; every other mode takes
exactly one source (`-source`, or the only enabled one) and `Config.ForSource`
collapses the config to a plain single-directory one before anything else
sees it. `api` and the stores never learn that sources exist. A config
without a `sources` block is the implicit single source, so nothing written
before this changes meaning.

**Wiring.** `main.go` builds the primary from `storage.primary`, wraps it with
the mirrors, and hands the result to `api` next to the driver
and the encryptor and hands it to every `api` entry point. `api` holds no
`os.*` call against backup_dir any more; the table maps what each site became.

| was (`api`)                                      | now                                   |
|--------------------------------------------------|---------------------------------------|
| `writeEncryptedArchive`: tmp file, fsync, rename | `store.Create` → encryptor → `Commit`; `Stat` first for the collision check |
| `writeFileSync(checksumPath)`                    | `interfaces.PutObject(.sha256)`       |
| `WriteMeta` / `ReadMeta`                         | `PutObject` / `GetObject`             |
| `ReadAllMeta`: `os.ReadDir` + prefix filter      | `store.List("backup_")` + suffix filter |
| `lineageOnDisk`: `os.Stat(archive)`              | `lineageInStore`: `store.Stat`        |
| `ExtractChain`, `EnsureState`: `Decrypt(path)`   | `decryptObject`: `store.Open` → `DecryptStream` |
| `removeBackupSet`, orphan loop: `os.Remove`      | `store.Delete`; `ErrObjectNotExist` keeps the counts honest |
| `findLatestEncryptedBackup`: mtime scan          | `findLatestArchive`: `List` + `ModTime` |
| `SweepStaleTemps` (backup_dir half)              | `store.Sweep`                         |
| `RunDryRun`: `MkdirAll` + `probeWritable`        | `probeStorage`: `PutObject` + `Delete` |

Three operator-visible consequences:

- `-file` names an archive in the store (`backup_<id>.zip.enc`). A path is
  still accepted for the file backend when it points into backup_dir.
- `-decrypt` takes a local file path *or* an archive name; a path that exists
  on disk wins. A bucket-held archive is decrypted straight from the store.
- Result JSON: `RetentionResult.storage` replaces `backupDir`;
  `DryRunResult.storage/storageOK/storageErr` replace `backupDirOK/Err`.
  `EncryptedFileName` and `ChecksumFile` are paths for the file backend and
  `s3://` URLs for S3.

With `storage.primary: s3`, `state_dir` is mandatory — the run lock cannot fall
back to backup_dir — and §7 item 2 gets sharper: two hosts can write one
bucket, the flock cannot see that, and `s3store.Sweep` only aborts uploads
older than 24 hours for exactly that reason. A bucket lifecycle rule for
incomplete multipart uploads is the belt to that braces.

`api/e2e_test.go` runs the real orchestration (full → incremental on a cold
cache → validate-only → retention → decrypt → dry run) through the file
backend, the S3 backend, and file mirrored to S3, with the real encryptor and
a fake driver. The S3 case uses the in-process
fake by default and the server named by `HOLDFAST_S3_TEST_ENDPOINT` when set.
