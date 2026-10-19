# SVOC Re-baseline Safety Design

## Goal

Make the re-baseline runsheet executable without the two SVOC driver scripts,
while keeping the runsheet, `svoc-rebase.sh`, and `svoc-flows.sh` operationally
consistent. The existing `dsync-progress.sh` and `dsync-find-pauses.sh` helpers
remain in use for monitoring and pause handling.

## Scope and assumptions

- The destination databases have already been dropped. Do not repeat the
  destructive drop during this task. Verify the expected empty destination
  state before launching the new flows.
- This work edits the runsheet and both SVOC driver scripts only, plus focused
  verification. It does not run commands against production infrastructure.
- The operator will refresh and validate the source collection inventory and
  the hot/cold/exclusion partition before generating env files.

## Design

1. **Standalone manual procedure:** make the runsheet's numbered steps
   self-contained and copy/pasteable. Add explicit checks for source inventory,
   complete hot/cold/exclusion coverage, no overlap, comma-separated namespace
   values, compose interpolation, and env-file permissions. Clearly mark the
   destination-drop step complete and non-repeatable for this run.
2. **Shared launch gates:** both drivers must fail closed if Temporal cannot be
   queried, if `prod-svoc` is still running, if the destination is not empty,
   if required env files/runner flags are missing, or if Temporal does not
   become healthy. Validate lists and stream-slot capacity before launch.
3. **Cutover readiness:** define the same readiness criteria in scripts and
   runsheet, including buffer drain (`EventsRead == EventsWritten`), read-side
   catch-up against LSN, and confirmation that source writes have stopped.
   Do not use age of the last event as a substitute for source-write
   quiescence.
4. **Credential handling:** create/update generated env files with restrictive
   permissions and remove temporary files safely. Avoid relying on interactive
   shell aliases.

## Verification

- Bash syntax checks for both scripts.
- Focused mocked checks for fail-closed state gates, list coverage/overlap,
  env-file permissions, Temporal health, and readiness calculations.
- Manual consistency check against the runsheet and CDC topology playbook.
- No production commands or destructive operations are executed.
