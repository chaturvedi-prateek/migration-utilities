# SVOC Re-baseline Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SVOC re-baseline runsheet safely executable without the two SVOC driver scripts, and bring both drivers into agreement with its validation, launch, and cutover gates.

**Architecture:** Keep the manual process in `helpers/svoc-rebaseline-runsheet.md`, with complete commands and explicit operator checkpoints. Keep the two self-contained drivers deployable as individual files; duplicate only small validation logic where necessary. Use the existing `dsync-progress.sh` and `dsync-find-pauses.sh` helpers for monitoring.

**Tech Stack:** Bash, Docker Compose, Temporal CLI, mongosh, jq, standard POSIX utilities.

## Global Constraints

- Destination databases have already been dropped; do not repeat the drop for this run.
- Verify the destination is empty for all in-scope databases and `adiom-internal` immediately before launch.
- Require a successful source-preflight marker matching the four list files and no older than 24 hours before launch.
- Flows must have disjoint namespace sets whose union equals the current source inventory plus deliberate, signed-off exclusions.
- Never use `--reset-heartbeats`; unpause with `--reset-attempts` only.
- Hot stream capacity must be at least 2 slots per hot namespace; cold sizing remains 2 workers × 5 slots.
- The cold dashboard binds to `127.0.0.1:8083`; hot dashboard remains on port `8081`.
- Protect all files containing MongoDB URIs with mode `600`.
- Verification uses mocks and local temporary fixtures; do not run production commands.

---

## File map

- Modify `docdb-to-mongodb/migration-toolkit/helpers/svoc-rebaseline-runsheet.md`: authoritative manual procedure, including refresh/partition, env generation, pre-launch checks, monitoring and cutover criteria.
- Modify `docdb-to-mongodb/migration-toolkit/internal/svoc-rebase.sh`: resume-aware one-file driver; keep the completed drop step callable but do not direct this operator to repeat it.
- Modify `docdb-to-mongodb/migration-toolkit/internal/svoc-flows.sh`: align its env/up/status commands with the same validation and fail-closed launch gates.
- Create `docdb-to-mongodb/migration-toolkit/internal/test-svoc-rebaseline.sh`: ignored, local-only mock tests for list validation, env-file modes, Temporal failure behavior, launch gates, and readiness criteria.
- Modify `docdb-to-mongodb/migration-toolkit/helpers/dsync-find-pauses.sh` and `helpers/dsync-progress.sh`: recognize Temporal's mixed-case `PauseRequested` state.

### Task 1: Make the standalone runsheet authoritative

**Files:**
- Modify: `docdb-to-mongodb/migration-toolkit/helpers/svoc-rebaseline-runsheet.md`

**Interfaces:**
- Consumes: installed toolkit at `/opt/docdb-migration`, source credentials in `compose/.env`, verified T2 hot list.
- Produces: commands an operator can execute without `svoc-rebase.sh` or `svoc-flows.sh`.

- [ ] **Step 1: Record the completed destination drop without repeating it.**

Replace the destructive drop command block in §2.3 with a state check that prints the in-scope DBs still present and stops unless none remain. Keep `adiom-internal` in the expected-absent set. Do not invoke `dropDatabase` in this run's manual path.

```bash
cd /opt/docdb-migration/compose
DBS=$(cut -d. -f1 ~/cdc-plan/all.txt | sort -u)
DEST=$(grep '^MDB_DEST=' .env | tail -n1 | cut -d= -f2-)
[[ -n "$DEST" ]] || { echo 'MDB_DEST missing' >&2; exit 1; }
mongosh "$DEST" --quiet --eval "
  const want=$(printf '%s\n' "$DBS" | jq -Rsc 'split("\n")[:-1]')
    .concat(['adiom-internal']);
  const have=new Set(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));
  const left=want.filter(d=>have.has(d));
  printjson({inScopeStillPresent:left});
  if (left.length) quit(2);"
```

Expected: `{ inScopeStillPresent: [] }`; a non-empty result or command failure stops the procedure. Do not print either URI.

- [ ] **Step 2: Make source inventory and partition validation a required gate.**

Document a fresh `all.txt` enumeration, then generate `cold.txt` from the current all/hot/exclude lists and run exact overlap and coverage checks against sorted unique lists. Require explicit sign-off for exclusions. Keep commands compatible with the host Bash and state plainly that all namespace lists are one namespace per line until joined with commas for `NAMESPACE`.

```bash
cd ~/cdc-plan
[[ -e exclude.txt ]] || : > exclude.txt
for f in all.txt hot.txt cold.txt exclude.txt; do
  [[ -f "$f" ]] || { echo "missing $f" >&2; exit 1; }
  sed -i 's/\r$//' "$f"
done
for f in all.txt hot.txt cold.txt; do
  grep -q . "$f" || { echo "$f is empty" >&2; exit 1; }
done
sort -u all.txt > all.sorted.txt
sort -u hot.txt > hot.sorted.txt
sort -u exclude.txt > exclude.sorted.txt
comm -23 all.sorted.txt <(cat hot.sorted.txt exclude.sorted.txt | sort -u) > cold.txt
sort -u cold.txt -o cold.txt
comm -12 <(sort -u hot.txt) <(sort -u cold.txt) > overlap.txt
[[ ! -s overlap.txt ]] || { cat overlap.txt; exit 1; }
sort -u hot.txt cold.txt exclude.txt > covered.txt
diff -u all.sorted.txt covered.txt
```

Expected: empty overlap and zero exit from `diff`; missing, duplicate-sensitive, or stale coverage is corrected before proceeding.

- [ ] **Step 3: Document safe environment generation.**

Replace the runsheet's temp-file `setenv` example with a mode-preserving update procedure that explicitly enforces mode `600`, removes `.env.tmp`, uses `command cp`/`command mv` to avoid interactive aliases, writes comma-separated lists, binds cold dashboard to loopback, and validates each count and Compose interpolation for all three env files. Use this exact update function after `umask 077`:

Use the following after setting `HOT` and `COLD` from the validated line files:

```bash
umask 077
command cp -f .env .env.prod-svoc.bak
chmod 600 .env.prod-svoc.bak
HOT=$(sed '/^[[:space:]]*$/d' ~/cdc-plan/hot.txt | paste -sd, -)
COLD=$(sed '/^[[:space:]]*$/d' ~/cdc-plan/cold.txt | paste -sd, -)
setenv() {
  local f="$1" kv k t
  for kv in "${@:2}"; do
    k="${kv%%=*}"
    t="$(mktemp "${f}.tmp.XXXXXX")" || return 1
    chmod 600 "$t"
    grep -v "^${k}=" "$f" > "$t" || true
    printf '%s\n' "$kv" >> "$t"
    chmod 600 "$t"
    command mv -f "$t" "$f" || { rm -f "$t"; return 1; }
  done
  chmod 600 "$f"
}
```

Finish the section with `chmod 600 .env .env.hot-streams .env.cold .env.prod-svoc.bak`, `stat -c '%a %n'` assertions, namespace-count comparisons, and `docker compose --env-file FILE config` for each file/project.

- [ ] **Step 4: Add independent pre-launch and Temporal-health checks.**

Before any worker or runner command, show commands to query Temporal health, ensure `prod-svoc` is absent from Running workflows, recheck the destination emptiness from Step 1, confirm exactly one runner `--stream-queue-name` and one `--max-partitions=500`, and ensure the hot worker-slot product is at least twice the validated hot count.

```bash
docker compose exec -T temporal temporal operator cluster health
docker compose exec -T temporal temporal workflow list \
  --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'"
```

Expected: health succeeds and the workflow list contains no `prod-svoc`. Do not proceed on command failure.

- [ ] **Step 5: Align monitoring and cutover criteria.**

Retain `dsync-progress.sh` / `dsync-find-pauses.sh`; state they are the only remaining helper-script dependencies. Replace the ambiguous readiness wording with: source writes stopped and independently confirmed; each partition has `EventsRead == EventsWritten`, `LSN - EventsRead == 0` (or an explicitly verified equivalent if LSN heartbeat timing is behind), and values remain drained across repeated samples. Do not equate age of `LastEventTime` with backlog after source writes stop.

- [ ] **Step 6: Review document consistency.**

Run `grep -nE 'dropDatabase|reset-heartbeats|DASHBOARD_PORT|EventsRead|LSN|mode 600|COVERAGE' docdb-to-mongodb/migration-toolkit/helpers/svoc-rebaseline-runsheet.md` and reconcile all occurrences with the completed-drop assumption, loopback port, no-heartbeat-reset rule, and updated gate.

### Task 2: Harden `svoc-rebase.sh` and align it with the manual procedure

**Files:**
- Modify: `docdb-to-mongodb/migration-toolkit/internal/svoc-rebase.sh`

**Interfaces:**
- Consumes: `all.txt`, `hot.txt`, `cold.txt`, optional `exclude.txt`, compose `.env`, Temporal CLI, mongosh.
- Produces: validated env files and gated `bringup`, with destination drop not required for the current operator path.

- [ ] **Step 1: Remove misleading unreachable duplicate command arms.**

Replace the grouped `prechecks|plan|compose-flags` stub plus unreachable implementations with explicit completed-step responses or safely callable commands. Keep `plan` available so it can refresh `all.txt` and recompute `cold.txt`; do not silently claim coverage validation if only file presence was checked. The operator path must not call `drop-dest` for this already-dropped destination.

- [ ] **Step 2: Add one reusable namespace-partition validation function.**

Use a function that normalizes CRLF, rejects empty or malformed lists, rejects overlap, and checks exact coverage of `all.txt` against `hot.txt + cold.txt + exclude.txt`. Invoke it from both `env-files` and `bringup` so stale files cannot bypass validation.

```bash
validate_partition() {
  local tmp="$PLAN_DIR/.svoc-validate.$$"
  mkdir -p "$PLAN_DIR"
  for f in all.txt hot.txt cold.txt exclude.txt; do
    if [[ "$f" == exclude.txt && ! -f "$PLAN_DIR/$f" ]]; then : > "$PLAN_DIR/$f"; fi
    [[ -f "$PLAN_DIR/$f" ]] || { rm -rf "$tmp"; die "missing $PLAN_DIR/$f"; }
    tr -d '\r' < "$PLAN_DIR/$f" | sed '/^[[:space:]]*$/d' | sort -u > "$tmp.$f"
  done
  [[ -s "$tmp.all.txt" && -s "$tmp.hot.txt" && -s "$tmp.cold.txt" ]] || die "all/hot/cold namespace lists must be non-empty"
  comm -12 "$tmp.hot.txt" "$tmp.cold.txt" > "$tmp.overlap"
  [[ ! -s "$tmp.overlap" ]] || { cat "$tmp.overlap" >&2; die "hot/cold overlap"; }
  cat "$tmp.hot.txt" "$tmp.cold.txt" "$tmp.exclude.txt" | sort -u > "$tmp.covered"
  diff -u "$tmp.all.txt" "$tmp.covered" || die "hot + cold + exclude do not cover all.txt"
  rm -f "$tmp".*
}
```

The implementation must also reject names containing commas or whitespace, or names with no `db.collection` separator. Require every hot/cold/exclude name to match an entry in `all.txt` exactly; collection names may themselves contain periods, so do not require exactly one period in the full namespace.

- [ ] **Step 3: Make env-file creation restrictive and robust.**

Ensure `umask 077` is active before backups/copies; use `mktemp` in the same directory, preserve atomic replacement, clean temporary files with a trap, then `chmod 600` all four env files. Verify mode and comma count after writing. Keep the existing hot/cold queue assignments and `127.0.0.1:8083` cold binding.

- [ ] **Step 4: Make state gates fail closed at both env generation and bring-up.**

Keep the Temporal list/status query failure distinct from “workflow absent”; check destination emptiness for the in-scope DBs plus `adiom-internal` immediately before starting workers/runners; require Compose health and configuration checks; ensure `prod-svoc` is not Running. No error in a query may be converted to a clean-state result.

- [ ] **Step 5: Preserve drop command safety without invoking it for this run.**

Keep `drop-dest` an explicit, typed-confirmation operation for future use; retain the source/destination host guard and post-drop verification. Its idempotent “already empty” path must check `adiom-internal` too. Do not make `env-files` call it automatically.

- [ ] **Step 6: Correct `ready`.**

For every `dsync_StreamChanges` heartbeat, require numeric `EventsRead`, `EventsWritten`, and matching LSN; calculate buffer backlog `EventsRead - EventsWritten` and read backlog `LSN - EventsRead`; fail closed on missing or malformed heartbeat data. Require both backlogs to be zero in repeated samples and require the operator to confirm source writes are stopped. Do not gate on `now - LastEventTime <= 60` as the sole lag test.

### Task 3: Harden `svoc-flows.sh` to the same contract

**Files:**
- Modify: `docdb-to-mongodb/migration-toolkit/internal/svoc-flows.sh`

**Interfaces:**
- Consumes: same plan files and compose `.env` as `svoc-rebase.sh`.
- Produces: env/up/status commands with matching validation and launch behavior.

- [ ] **Step 1: Add the same partition validation to `env` and `up`.**

Use the validation contract from Task 2: current `all.txt`, non-empty hot/cold, valid namespaces, no overlap, and complete coverage with exclusions. Fail before modifying `.env` if any check fails.

- [ ] **Step 2: Fix credential permissions in `setenv`.**

Set `umask 077`; create temp files with `mktemp` in the env file's directory; install cleanup with `trap`; atomically replace using `command mv -f`; enforce `chmod 600` after copies and replacements. Verify resulting modes before printing success.

- [ ] **Step 3: Make `up` fail closed and verify state immediately before launch.**

Capture and check the exit status of Temporal queries rather than relying on a pipeline's `grep` result. Check Temporal health and abort unless it succeeds. Recheck that `prod-svoc` is not Running and that every in-scope destination database plus `adiom-internal` is absent. Validate compose configs, required runner flags, and hot slot capacity before issuing any `up` command.

- [ ] **Step 4: Keep dashboard and pool configuration aligned.**

Retain hot dashboard `8081`, cold dashboard `127.0.0.1:8083`, hot stream pool 4×20 slots, hot sync pool 3×4, cold pool 2×5, and the current separate stream queue routing. Add a bring-up post-check that confirms both new workflow IDs appear Running; if either fails, report partial startup and do not claim success.

### Task 4: Add focused local safety tests and verify the complete change

**Files:**
- Create: `docdb-to-mongodb/migration-toolkit/internal/test-svoc-rebaseline.sh`
- Modify: `docdb-to-mongodb/migration-toolkit/helpers/dsync-find-pauses.sh`
- Modify: `docdb-to-mongodb/migration-toolkit/helpers/dsync-progress.sh`
- Modify: `docdb-to-mongodb/migration-toolkit/helpers/svoc-rebaseline-runsheet.md`
- Modify: `docdb-to-mongodb/migration-toolkit/internal/svoc-rebase.sh`
- Modify: `docdb-to-mongodb/migration-toolkit/internal/svoc-flows.sh`

**Interfaces:**
- Consumes: both scripts and mock `docker`, `mongosh`, and `jq` executables placed first in `PATH`.
- Produces: deterministic local evidence that failure states stop before launch and generated files are protected.

- [ ] **Step 1: Create a temporary-fixture test harness.**

The test must create a temp root, install cleanup with `trap`, create fake source inventory (`db.hot`, `db.cold`, `db.excluded`), and fake commands that return deterministic Temporal/DB states. Use these exact assertion helpers:

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
PASS=0
FAIL=0
assert_status() {
  local name="$1" want="$2" got="$3"
  if [[ "$got" == "$want" ]]; then PASS=$((PASS + 1)); else
    echo "FAIL $name: wanted $want, got $got" >&2
    FAIL=$((FAIL + 1))
  fi
}
```

- [ ] **Step 2: Exercise namespace and permissions cases.**

Test valid complete lists, an overlap, a missing namespace, an empty hot list, malformed `db.collection`, and generated env modes. Expected: only the valid partition passes; env files are exactly mode `600`; any invalid partition exits non-zero before invoking mocked compose `up`.

- [ ] **Step 3: Exercise fail-closed launch gates.**

Mock Temporal states for Running, not Running, and command failure; mock healthy/unhealthy Temporal; mock destination DB list empty/non-empty/query failure. Expected: only “old workflow absent + Temporal healthy + destination empty + valid config” reaches the first mocked worker launch. Query failure, unhealthy Temporal, or a leftover DB exits non-zero with zero launch calls.

- [ ] **Step 4: Exercise readiness calculations.**

Use heartbeat fixtures for: buffer drained and LSN caught up; nonzero read backlog; nonzero write backlog; missing LSN; and stale last-event timestamp with both counters drained. Expected: only the first is ready, subject to confirmed source-write stop; stale event age alone must not reject a drained stream.

- [ ] **Step 5: Run verification.**

```bash
bash -n docdb-to-mongodb/migration-toolkit/internal/svoc-rebase.sh
bash -n docdb-to-mongodb/migration-toolkit/internal/svoc-flows.sh
bash -n docdb-to-mongodb/migration-toolkit/helpers/dsync-find-pauses.sh
bash -n docdb-to-mongodb/migration-toolkit/helpers/dsync-progress.sh
bash docdb-to-mongodb/migration-toolkit/internal/test-svoc-rebaseline.sh
```

Expected: syntax checks succeed; test output ends with `fail=0`. Also inspect the generated Compose config using fixtures only and verify no test command targets production URIs.

- [ ] **Step 6: Self-review manual/script parity.**

Compare preflight, partition validation, env files, slot sizing, dashboard binding, launch guards, unpause guidance, and ready criteria in the runsheet and both scripts. Resolve any mismatch before returning the completed work.
