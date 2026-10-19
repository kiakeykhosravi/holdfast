# Retention: state of play and handoff

Written 2026-10-17, after the storage seam (Storer, mirrors, `-sync-mirrors`)
landed and before any retention redesign. Read this before touching
`api/retention.go`; it records what exists, which guarantees must survive, and
the shape the replacement should take. Architecture.md §7 is the shorter
version of the same position.

## 1. What exists

`api.CleanupRetentionStats(ctx, store, days, key)` in `api/retention.go`,
called under the run lock by `api.RunRetention` and `api.RunRetentionOnly`.
`main.go` runs it after every backup run, whether or not the backup step
succeeded; only a skip or a cancellation bypasses it. `-retention-only` runs
it alone; `-retention-days n` overrides the config for one run;
`RETENTION_DAYS` overrides the file; `retention_days: 0` disables it.

The algorithm, exactly:

1. `ReadAllMeta` the primary store. If any sidecar is in `Skipped`
   (unparseable, invalid, or signature does not verify), **return an error
   and delete nothing**.
2. Group the readable sidecars by `ChainID` (`MetaScan.Chains()`, oldest
   first within a chain).
3. For each chain, look at its **newest** member's `CreatedAt`. If it is not
   older than `now - days*24h`, keep the whole chain. Otherwise delete every
   member: archive, then `.sha256`, then sidecar, in that order
   (`removeBackupSet`). Sidecar last, so a crash mid-delete leaves a
   detectable orphan rather than a sidecar pointing at a missing archive.
4. Archives and checksums with **no** sidecar (pre-sidecar backups, or a run
   that died between commit and `WriteMeta`) are governed by the timestamp
   in their name (`parseBackupTime`, UTC) alone: nothing can depend on them,
   because parent selection reads sidecars only.
5. Report `RetentionStats{ChainsDeleted, BackupsDeleted, FilesDeleted,
   ChainsKept, OrphansDeleted}`.

Deletions go through the `Storer`, so with `storage.mirrors: [s3]` they fan
out to the bucket (a missing mirror object is not an error). Without
mirrors, `-sync-mirrors -prune` on the NFS server later makes the bucket
match the primary.

Tests: `api/retention_chain_test.go` (live chain survives; expired chain
deleted whole incl. sidecars; refuses on an unreadable sidecar; undescribed
archives age out; `days: 0` inert), `api/meta_sign_test.go` (a tampered
sidecar stops retention), `api/sync_test.go` (prune follows the primary and
refuses on an empty or untrusted primary).

## 2. Invariants that must survive any redesign

These each closed a real data-loss or availability bug. Breaking one is a
regression even if every new feature works.

1. **Chain-whole-or-nothing.** A full is never deleted while any incremental
   built on it is kept. The unit of retention is the chain, keyed by
   `ChainID`, never the individual backup. (The filename-only predecessor
   deleted fulls out from under their incrementals.)
2. **Decide on the newest member.** A chain's age is its newest member's
   age. This keeps chains slightly longer than a per-backup policy would,
   which is the right direction to err.
3. **Refuse on untrusted sidecars.** Any `MetaScan.Skipped` entry aborts
   the pass. An unreadable sidecar describes a backup whose dependents are
   unknown.
4. **Delete the set together, sidecar last.** Archive, checksum, sidecar.
   A surviving sidecar for a deleted archive poisons parent selection and
   chain validation.
5. **Under the run lock.** Retention reads the same chain bookkeeping a
   concurrent backup writes. `RunRetention` takes the lock; contention is a
   skip.
6. **Runs regardless of the backup's outcome.** A failing restore test must
   not stop cleanup, or a stuck validation container fills the disk with
   fulls.
7. **Undescribed archives age out by name.** Otherwise a crash between
   commit and `WriteMeta` leaves a cluster-sized file forever.
8. **The primary is the truth.** Mirrors follow the primary (fan-out delete,
   or sync `-prune`). Nothing decides retention from a mirror's listing.
9. **Nothing deletes on an empty or untrusted listing.** Sync `-prune`
   refuses when the primary lists no backups (unmounted share) or has an
   unreadable sidecar. A retention policy that could ever conclude "delete
   everything" must have the same guard.

## 3. What is missing

- **No GFS.** No "keep 7 dailies, 4 weeklies, 12 monthlies, 3 yearlies".
  `retention_days` is the only knob.
- **No minimum.** Nothing stops `retention_days: 1` plus a failed night from
  deleting the last good backup. A `min_chains_kept` floor is the cheapest
  safety the current code lacks.
- **No per-destination policy.** The bucket lives and dies with the primary.
  "Offsite keeps a year, local keeps two weeks" is impossible today, and
  faking it with a longer `retention_days` on the sync host is wrong,
  because `-prune` deletes what the primary lacks on the NFS server's
  schedule.
- **No preview.** There is no "what would retention delete right now".
- **Age is wall-clock days.** No calendar awareness, so DST, month ends and
  missed runs are not reasoned about.
- **No metrics.** Only the JSON on stdout.

## 4. The intended shape

Already sketched in Architecture.md §7; this is the fuller version.

### 4.1 Split decision from action

```go
// Pure. No clock, no filesystem, no config access beyond policy.
func Plan(chains []Chain, policy Policy, now time.Time) (*RetentionPlan, error)

// Does the I/O. Takes the plan, not the policy.
func Apply(ctx, store, plan) (RetentionStats, error)
```

`Chain` is `MetaScan.Chains()` reshaped: `{ID string; Members []BackupMeta}`
with the root first. `RetentionPlan` lists, per chain, *keep* or *delete* and
the reason (`"expired: newest member 2026-09-01 older than 14d"`,
`"kept: monthly for 2026-09"`, `"kept: min_chains_kept"`). Purity is what
makes DST boundaries, month ends, clock skew and "a run was missed for three
days" table-testable with fixed `now` values.

`-dry-run` and a new `-retention-only -plan` (or `-dry-run` honouring
retention) print the plan without applying it. That closes the preview gap
for free.

### 4.2 Policy

```yaml
retention:
  days: 14             # today's rule, kept as the floor: nothing younger is ever deleted
  keep_daily: 7
  keep_weekly: 4
  keep_monthly: 12
  keep_yearly: 3
  min_chains_kept: 2   # never go below this many chains, whatever the calendar says
  week_starts: monday
  timezone: UTC        # calendar boundaries; IDs are UTC, CreatedAt carries its own zone
```

Keep `retention_days` working as an alias of `retention.days` so existing
configs and `RETENTION_DAYS` / `-retention-days` do not break.

### 4.3 Classify at expiry time, on chains

Do **not** tag a backup "monthly" when it is taken. Tag-at-creation breaks
the first time a scheduled run is missed: the first-of-the-month backup does
not exist and the slot is empty forever. Instead, at plan time:

- For each tier (daily, weekly, monthly, yearly) walk the calendar buckets
  backwards from `now`; in each bucket keep the **oldest chain whose newest
  member falls in that bucket** (the conventional GFS choice; the newest
  would also be defensible, pick one and document it).
- A chain is kept if any tier claims it, or if it is younger than
  `retention.days`, or if it is among the `min_chains_kept` newest.
- Everything else is deleted, whole.
- The unit is the chain and the chain's date is its newest member. Keeping a
  "monthly" therefore keeps a full plus its incrementals, which is more than
  one archive; restore time and disk follow `max_chain_length`. Note this in
  the config comment.

### 4.4 Per-destination policy (if wanted)

Two clean ways, neither built:

1. **Retention on the mirror as its own primary.** On the sync host run
   `-retention-only` with a second config whose `primary: s3` and a longer
   policy, and run `-sync-mirrors` **without** `-prune`. The bucket then
   keeps its own schedule. This works with today's code except that sync
   must not prune. Document the pairing, or make `-prune` refuse when a
   `retention:` block exists in the same config, to stop the two from
   fighting.
2. **Policy per store in one config.** `storage.s3.retention: {...}` and a
   plan per destination. More machinery; only worth it if one host must own
   both decisions.

Either way the invariant in §2.8 changes shape: each store becomes the truth
for its own policy, and sync `-prune` must be disabled for that store.

### 4.5 Safety rails the plan must carry

- Refuse to produce a plan that deletes every chain, unless the policy is
  explicitly `keep_nothing: true` or similar. (§2.9.)
- Refuse when any sidecar is untrusted. (§2.3.)
- Never delete a chain containing a backup younger than `retention.days`.
- `min_chains_kept` applies after every other rule.
- Log every delete decision with its reason; print the plan in the JSON.

## 5. Where the code changes

| file | change |
|---|---|
| `types/config.go` | `RetentionPolicy` struct, `retention:` block, alias for `retention_days`, validation (non-negative counts, known timezone, `min_chains_kept >= 1`) |
| `types/cli_results.go` | `RetentionResult` gains the plan summary (kept/deleted per tier, reasons) |
| `api/retention.go` | `Plan` + `Apply`; `CleanupRetentionStats` becomes `Apply(Plan(...))`; `removeBackupSet` and the undescribed-archive pass stay as they are |
| `api/meta.go` | a `Chains() []Chain` that returns ordered chains with their dates, if the map form is awkward for the planner |
| `api/dry_run.go` | include the retention plan |
| `main.go` | flags `-retention-days` stays; maybe `-retention-plan`; keep "retention runs regardless of backup outcome" |
| `api/sync.go` | `-prune` guard against a per-store policy, if §4.4 option 1 is chosen |
| `config.yaml`, `README.md`, `Architecture.md §7` | document the policy and the chain-is-the-unit consequence |

Tests to write before the implementation, as tables over fixed `now`:

- 30 daily chains, policy 7/4/12/3: exactly which survive, by ID.
- A missed week: weekly slot falls to the next available chain, not empty.
- Month with five Mondays; February; year boundary; DST transition in a
  non-UTC `timezone`.
- `min_chains_kept` beats an aggressive calendar.
- All chains expired, `min_chains_kept: 0`: plan refused.
- Mixed media: a logical daily job and a physical weekly job in one store
  are separate chains and must each get their own tiers (or document that
  tiers are per medium — decide).
- An untrusted sidecar: no plan.
- Existing `retention_chain_test.go` unchanged and still green.

## 6. Open decisions (decide before coding)

1. Oldest or newest chain per calendar bucket?
2. Tiers per store, or per medium within a store? A store with daily logical
   and weekly physical chains needs an answer.
3. Per-destination policy: §4.4 option 1 (two configs, no prune) or option 2
   (one config, plan per store)?
4. Does `-dry-run` print the plan, or is it a separate flag?
5. Should `-retention-only` require the passphrase so sidecar signatures are
   verified, or keep today's "warn and proceed unsigned" behaviour?
