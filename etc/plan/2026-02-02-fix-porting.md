# Fix porting bugs (clobber + missing import)

## Context
The `gotwc/internal/porting` package handles import/export between the filesystem and Got FS. There are two correctness bugs that must be fixed first, before larger design changes:

1. **Incorrect clobber detection** during export/checkout.
2. **Missing import** when we should re-import changed file contents.

This plan is a brain dump for the next agent to implement.

---

## Bug 1: Export clobbers untracked files

### Symptom
When exporting a file from Got to the worktree (via `Exporter.exportFile`), if a file exists on disk but is **not** present in the porting `dirstate` DB, it still gets overwritten. This is inconsistent with Git semantics: untracked files should block checkout/export unless forced.

### Root cause
`Exporter.exportFile` only checks `dirstate` when the file already exists on disk. It calls `db.GetInfo` and only raises `ErrWouldClobber` if it **finds an entry** and detects metadata changes. If there is no DB entry, it proceeds and overwrites.

### Fix strategy
- Treat “file exists on disk but no `dirstate` entry” as **untracked**, and return `ErrWouldClobber`.
- Apply the same logic to delete operations in `deleteFile` and `deleteDir` (to avoid removing untracked files).

### Specific code target
`got/src/gotwc/internal/porting/exporter.go`

### Sketch
- In `exportFile`:
  - If `finfo != nil` (file exists) and `db.GetInfo` returns `found == false`, return `ErrWouldClobber{Op: "write", Path: p}`.
  - If `found == true` but `HasChanged`, return `ErrWouldClobber` (already done).
- In `deleteFile`:
  - Already returns `ErrWouldClobber` if no DB entry; keep this behavior.
- In `exportDir` / `deleteDir`:
  - Ensure removal of children uses `deleteFile` so untracked files block.

### Tests to add
- Create file on disk outside of `dirstate`, then export snapshot that would overwrite it. Expect `ErrWouldClobber`.
- Create untracked file in directory that is removed on export. Expect `ErrWouldClobber` instead of deletion.

---

## Bug 2: Importer skips re-import when file has changed

### Symptom
`Importer.importFile` sometimes reuses cached file roots for files that have actually changed, causing stale content to be staged/committed.

### Root cause
The caching condition is inverted. The current logic:
```
if ok && HasChanged(&ent, finfo) {
    // "using cache entry ... skipped import"
    return cached root
}
```
This is backwards. It reuses the cached root when the file has changed, which is exactly when it should re-import.

### Fix strategy
- Only use cached `fsroot` when **the file has not changed**.
- If metadata differs, re-import the file and update `dirstate` + `fsroots`.

### Specific code target
`got/src/gotwc/internal/porting/importer.go`

### Sketch
- Change the conditional to:
  - `if ok && !HasChanged(&ent, finfo) { return cached root }`
- Ensure that a cache hit still verifies `fsroot` exists; if missing, fall back to import.
- Ensure the path returns correct `PutInfo` + `PutFSRoot` on actual import.

### Tests to add
- Import file, modify it, import again, ensure different content is stored.
- Modify file metadata without content change (touch) to verify cache is not incorrectly reused (expected behavior: we likely re-import based on modtime/size/mode).

---

## Implementation notes / gotchas
- `HasChanged` uses modtime/size/mode; keep this for now (metadata-only). Deeper hashing/content checks are out of scope.
- Be careful of directory entries: `importFile` is for regular files only; ensure stat validation stays.
- Make sure the DB is updated correctly on successful export/import to keep state consistent.

---

## Out of scope
- Redesign of dirty detection to be Git-like.
- Preflight export to avoid partial mutations.
- Handling directory modtime noise.
- Tracking-span filtering for `dirstate`.

---

## Acceptance checklist
- Export refuses to overwrite/delete untracked files.
- Import uses cached root only when unchanged.
- Unit tests cover both bug classes.
- All new tests should be table tests, and in the package closest to the functionality that they test. e.g. `gotwc/internal/porting` is a good place for a test, and if that doesn't work, then `gotwc` is the next best place.
