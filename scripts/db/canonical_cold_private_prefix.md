# Prepare a reviewed cold prefix on the private clone

Use the promoted rehearsal clone, its placement receipt, and the separately
seeded candidate archive. Prepare the empty cold catalog on the registered
private HDD target first. Use the exact native Unix socket from the clone
proof. Run in the network-disabled capsule with the same bounded HDD I/O
settings used to boot the clone.

Keep the candidate archive mount path the same for every invocation. Bind
it read-only except for `stage`. Bind an existing, private, writable sibling
directory on the same HDD as the work mount. It holds the SQLite migration
checkpoint and all stage, load and audit cursors. Reuse it after an interruption.

Run `plan` with the inclusive `--through-day` and inspect the resulting dated
and legacy physical children and each day's retention marker. For example,
`--through-day 2026-10-17` requires the entire legacy child and every applicable
dated day through October 17. Pass the reviewed `prefix_sha256` back as
`--prefix-sha256` for all later actions. The plan refuses to split an attached
legacy child or to skip earlier historical days.

All actions take `--proof`, `--proof-sha256`, `--placement`,
`--placement-sha256`, `--work-root`, `--through-day`, `--min-free-bytes`, and
`--batch-work-bytes`. Set `PG_DSN` and the archive UUID environment as for the
private loader. For `seal` set `MARKET_STRUCTURE_STORAGE_ROOT` to the original
private archive root. Use the candidate archive root for the other actions.

1. Run `stage` until it reports completion. It verifies existing canonical
   pages from the seeded archive or creates new pages from the sealed SQL
   tail. It stops at the first open day. To seal a reviewed dated day, use
   `seal --seal-day DAY --header-oid OID --payload-oid OID
   --source-archive-object-root PATH --count-statement-timeout-ms LIMIT`.
   Bind the original verified archive objects at `PATH` read-only. The work
   journal records the open marker before the native seal and preserves the
   exact row count and timestamp after commit.
2. Run `load` with `--load-limits`, `--load-limits-sha256` and the original
   native `--pg-controldata`. Each page uses the established shared-HDD
   PostgreSQL capacity watch. A deferred load can resume from its previous
   committed page.
3. Run `audit` until both the forward day receipts and reverse loaded inventory
   traversal complete. Both cursors persist after each batch.
4. Run `export`. It validates the complete receipt set and publishes
   `audit-receipts.json` and `seal-receipts.json` under the work root. Use the
   reported file SHA-256 values when mounting them read-only for activation
   preparation and private rollback admission.

Use `--max-batches`, `--pages-per-batch`, and `--max-seconds` to bound one
invocation. Staging admits one page at a time and requires room for two encoded
pages plus the reviewed HDD free-space floor and registered reservations.
Floor samples bracket object staging. The PostgreSQL loader separately watches
its own temporary files, WAL, and catalog allocations. No phase activates
new canonical authority.
