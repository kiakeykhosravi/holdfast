# Holdfast — operating guide

**Holdfast** keeps your databases restorable. Every backup is an encrypted
archive, a checksum, and an HMAC-signed sidecar that records what it is, what
chain it belongs to, and whether a restore test passed. A directory or bucket
of sidecars is the whole index, so any copy of your backups is enough to
recover from. Drivers plug in per database engine (PostgreSQL today; MySQL,
MSSQL, MongoDB and ClickHouse planned); storage plugs in per backend, with
mirrors kept in sync.

How to build, configure and run the tool, host by host and mode by mode. For
how the code is organised and why, see `Architecture.md`.

## Build

```
cd holdfast
go build -o holdfast .
```

One static binary. Copy it to every host that takes part. Run the tests with
`go test ./...`; the S3 tests use an in-process fake unless
`HOLDFAST_S3_TEST_ENDPOINT` points them at a real server.

## The pieces

A backup is three objects that always travel together:

| object | what it is |
|---|---|
| `backup_<id>.zip.enc` | the encrypted archive (AES-256-CTR + HMAC, scrypt key) |
| `backup_<id>.zip.enc.sha256` | SHA-256 of the ciphertext |
| `backup_<id>.meta.json` | the plaintext, HMAC-signed sidecar: type, chain, parent, validation verdict |

`<id>` is a UTC timestamp, `2026-10-17_03-00-00`. The sidecars are the
index: a directory or bucket of them is restorable with nothing else.

Three directories, two of which are always local:

| setting | role | may be shared? |
|---|---|---|
| `backup_dir` | where the three objects live (the **primary** store when `storage.primary: file`) | yes, NFS is typical |
| `state_dir` | run lock (flock) and the parent-manifest cache | **no**, local disk |
| `scratch_dir` | staging for `pg_basebackup`, chain extraction, restore tests; cluster-sized | **no**, local disk, not tmpfs |

## Secrets

Never in the config file. Environment only:

| variable | needed by |
|---|---|
| `BACKUP_PASSPHRASE` | backup, dry-run, decrypt, test-only; refused from the config file |
| `PG_CONN` | anything that talks to Postgres (overrides `pg_conn_string`) |
| `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | any host with S3 as primary or mirror; leave both unset to use the AWS SDK chain (`AWS_*`, `~/.aws`, instance role) |

`sudo` resets the environment, so `sudo holdfast ...` sees none of these.
Use `sudo env VAR=... holdfast ...` or `sudo -E`, or run it as a service,
where `EnvironmentFile` supplies them. A missing S3 key fails at startup
with exit 2 and a message saying so.

Other overrides: `BACKUP_DIR`, `STATE_DIR`, `SCRATCH_DIR`, `BACKUP_TYPE`,
`RETENTION_DAYS`, `MIN_CHAINS_KEPT`, `RUN_ON_ROLE`, `MAX_REPLICA_LAG_SECONDS`,
`ENABLE_RESTORE_TEST`, `BACKUP_STORAGE`, `BACKUP_STORAGE_MIRRORS`,
`S3_BUCKET`, `S3_PREFIX`, `S3_ENDPOINT`, `S3_REGION` (or `AWS_REGION`).

## Storage: primary and mirrors

```yaml
storage:
  primary: file        # file | s3 — authoritative; reads, parent selection and retention decisions come from it
  mirrors: []          # e.g. [s3] — also written to and deleted from
  s3:
    bucket: ""
    prefix: ""         # e.g. prod
    region: ""         # required for AWS; defaults to us-east-1 when endpoint is set
    endpoint: ""       # e.g. https://s3.example.internal; empty for AWS
    use_path_style: false   # true for most S3-compatibles
    legacy_compat: false    # true if the server rejects x-amz-checksum headers
    part_size_bytes: 0      # 0 = 64 MiB; archive size cap is 10000 parts
    concurrency: 0          # 0 = 4; memory ≈ part_size × concurrency
```

Rules that follow from this:

- The primary behaves exactly as `backup_dir` always did. A mirror is a copy.
- A mirror failing never fails the run. The primary completes, the failure is
  printed, and the exit code is 3.
- With a mirror the dump runs at the speed of the slowest destination.
- With `primary: s3`, `state_dir` is required.
- Set a bucket lifecycle rule that aborts incomplete multipart uploads after a
  day. The tool's own sweep only aborts uploads older than 24 hours.

## Several backup sets on one host: sources

A host that serves more than one cluster's backup directory, the NFS server
typically, lists them as sources and runs one process, one service, one
timer over all of them:

```yaml
state_dir: /var/lib/holdfast
storage:
  primary: file
  mirrors: [s3]
  s3: { bucket: my-backups, endpoint: https://s3.example.internal, use_path_style: true }
sources:
  - name: opb
    backup_dir: /nfs/data/opb_backups
  - name: pg
    backup_dir: /nfs/data/pg_backups
  - name: legacy
    backup_dir: /nfs/data/holdfast
    enabled: false
```

- Each source's objects go under its own prefix: `storage.s3.prefix` (if
  any) plus `/` plus the source name, or the source's `prefix` override.
  Config validation rejects two sources whose prefixes would coincide, so
  `-prune` can never delete another source's objects.
- `-sync-mirrors` runs every enabled source in turn under one run lock and
  prints one JSON document with a `sources` array and a total `failed`.
  A source that fails is reported and the next one still runs. Exit 3 if
  any object anywhere failed.
- `-source <name>` restricts any mode to one source, enabled or not. The
  single-source modes (`-retention-only`, `-test-only`, `-decrypt`, backup,
  `-dry-run`) require it when more than one source is enabled.
- A config without a `sources` block is unchanged: `backup_dir` is the one
  implicit source.

## Hosts and their configs

### Database host, local only

The old behaviour. Start from `config.yaml` and set:

```yaml
backup_dir: /mnt/nfs/backups
state_dir: /var/lib/holdfast
scratch_dir: /var/tmp/holdfast
backup_type: logical-full          # or physical-full / physical-incremental
run_on_cluster_role: replica       # or primary
enable_restore_test: true          # needs Docker and pg_test_docker_image = server major
retention_days: 14
pg_conn_string: "postgres://backup_user@host:5432/postgres"   # password via PG_CONN
storage:
  primary: file
  mirrors: []
```

### Database host, mirroring to S3 live

Only if the upload may share the database host's link. As above, plus:

```yaml
storage:
  primary: file
  mirrors: [s3]
  s3:
    bucket: my-backups
    prefix: prod
    endpoint: https://s3.example.internal
    use_path_style: true
```

### NFS server, sync host

Serves the backup directories and pushes them to S3. Needs nothing about
Postgres and no passphrase. One directory:

```yaml
backup_dir: /srv/backups
storage:
  primary: file
  mirrors: [s3]
  s3:
    bucket: my-backups
    prefix: prod
    endpoint: https://s3.example.internal
    use_path_style: true
```

Several directories: the `sources` form above, in one config, one service.

### Restore host, reading from S3

For when the NFS copy is gone. Archive names, not paths, for `-decrypt` and
`-file`:

```yaml
state_dir: /var/lib/holdfast
scratch_dir: /var/tmp/holdfast
pg_test_docker_image: postgres:18
storage:
  primary: s3
  s3:
    bucket: my-backups
    prefix: prod
    endpoint: https://s3.example.internal
    use_path_style: true
```

## Modes

Flags are mutually exclusive; with none, a backup runs.

| mode | flag | where | what it does | needs |
|---|---|---|---|---|
| dry run | `-dry-run` | database host | validates config, probes every storage destination with a throwaway object, takes and releases the lock, plans the parent, runs the Postgres preflight; writes no backup | full config, `BACKUP_PASSPHRASE`, DB reachable |
| backup | *(none)*, `-type`, `-parent` | database host | the pipeline, then retention as a separate locked step | full config |
| validate | `-test-only [-file name]` | database or restore host | decrypts the chain ending at the named (default: newest) archive, boots it in Docker, records the verdict in the sidecar; the verdict is what lets a later incremental chain onto it | storage, passphrase, Docker |
| decrypt | `-decrypt <path or name> [-out path]` | any host | verifies the checksum and writes the plaintext zip; a local path wins, otherwise the name is looked up in the primary store; defaults to the current directory and refuses to overwrite | storage, passphrase |
| retention | `-retention-only [-retention-days n]` | database host (or any host with the primary) | the chain-safe cleanup, under the lock; deletions fan out to mirrors | storage |
| sync | `-sync-mirrors [-prune] [-source name]` | NFS server, cron | for every enabled source (or the named one), copies what each mirror lacks, verifying archives against their `.sha256`; `-prune` also deletes what the primary no longer has | storage with at least one mirror |

Examples:

```
holdfast -config /etc/holdfast.yaml -dry-run
holdfast -config /etc/holdfast.yaml                       # cron, daily
holdfast -config /etc/holdfast.yaml -type physical-incremental
holdfast -config /etc/holdfast.yaml -test-only -file backup_2026-10-17_03-00-00.zip.enc
holdfast -config /etc/holdfast.yaml -decrypt backup_2026-10-17_03-00-00.zip.enc -out /tmp/x.zip
holdfast -config /etc/holdfast.yaml -retention-only -retention-days 30
holdfast -config /etc/holdfast-sync.yaml -sync-mirrors -prune    # cron, hourly, NFS server; all enabled sources
holdfast -config /etc/holdfast-sync.yaml -sync-mirrors -source opb
holdfast -config /etc/holdfast-sync.yaml -test-only -source opb   # needs passphrase, Docker, scratch_dir
```

## Exit codes

| code | meaning |
|---|---|
| 0 | success, or a healthy skip (wrong cluster role, lagging replica, another run holds the lock) — stdout JSON says which |
| 1 | the run failed; stderr says why |
| 2 | config or flags invalid; nothing ran |
| 3 | the primary succeeded but a mirror operation failed, or a sync left objects behind; retried next run |
| 130 | cancelled by signal |

Every run prints one JSON document on stdout. Monitoring should alert on
exit codes 1 and 3, and on a `skipped: true` result that persists for longer
than the schedule interval.

## Retention, today

Age-based and chain-safe, not GFS. A chain (a full and the incrementals built
on it) is kept whole until its **newest** member is older than
`retention_days`, then the whole chain is deleted, archive, checksum and
sidecar together. The newest `min_chains_kept` chains (default 1) are never
deleted, however old: retention runs after every backup attempt, failed or
not, and without the floor a run of failed nights longer than `retention_days`
would delete the last good backup. Retention refuses to run while any sidecar
is unreadable or fails its signature, because an unreadable sidecar describes
a backup whose dependents are unknown. `retention_days: 0` disables it;
`min_chains_kept: 0` disables the floor.

Consequences:

- `retention_days` and `min_chains_kept` are the only knobs. With daily
  incrementals and `max_chain_length: 7`, a chain lives roughly a week past
  that number. The result JSON reports `chainsKeptByFloor`; a non-zero value
  that persists means backups have stopped succeeding.
- There is no "keep one full per month". See `docs/retention-handoff.md`.
- Deletions reach S3 in one of two ways: retention running on a host whose
  config lists `mirrors: [s3]` (immediate), or `-sync-mirrors -prune` on the
  NFS server (next sync). Without either, the bucket only grows.
- S3 has no retention of its own. Do not try to give it a longer life with a
  different `retention_days` on the sync host: `-prune` follows the primary,
  so that would delete from the bucket on the NFS server's schedule.

## Sidecar signatures

Every sidecar is signed with a key derived from `BACKUP_PASSPHRASE`. A
sidecar whose signature does not verify is untrusted: parent selection skips
it and anything that deletes refuses to run. An **unsigned** sidecar is, by
default, accepted as one written before signing existed. That default is a
hole: whoever can edit a sidecar can also delete its `signature` field. Close
it in two steps:

1. Upgrade every host and let each run one backup. The run signs every
   unsigned sidecar it finds and logs how many.
2. Set `require_signed_sidecars: true`. From then on an unsigned sidecar is
   treated like a tampered one, and `-retention-only` and
   `-sync-mirrors -prune` refuse to delete unless they can verify, which on
   a sync host without the passphrase means prune is off (copying still
   works).

## First run checklist

1. `-dry-run` on the database host: `configOK`, `storageOK`, `lockAvailable`
   and `shouldRun` all true. If `shouldRun` is false, `shouldRunNote` says why.
2. A real `-type logical-full` with `enable_restore_test: false`; confirm the
   three objects in `backup_dir` (and the bucket, if mirrored).
3. `-decrypt` that archive by name into `/tmp` and look inside the zip.
4. Turn restore tests on, run `-test-only`; the sidecar gains
   `validationResult: ok`.
5. `-retention-only`: on a fresh setup it keeps everything.
6. On the NFS server, `-sync-mirrors`, then again: the second run copies
   nothing.
7. Put the schedules in cron and alert on exit codes 1 and 3.
