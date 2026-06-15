# Index Script Generation and Execution

## What this folder does

The script [createIndexDefinition.js](createIndexDefinition.js) connects to the current MongoDB shell context and generates two scripts:

1. `createIndexesAtDestination.js` (pre-cutover)
- Creates indexes on destination.
- Forces TTL indexes to `expireAfterSeconds: 2147483647`.
- Creates originally unique indexes as non-unique (`unique: false`).

2. `postCutoverRectifyIndexes.js` (post-cutover)
- Restores unique indexes using `collMod` with:
  - `prepareUnique: true`
  - then `unique: true`
- Restores original TTL values using `collMod` with original `expireAfterSeconds`.

## Prerequisites

1. `mongosh` installed and accessible in PATH.
2. Connectivity and permissions to:
- list databases
- list collections
- read indexes
- run `createIndex` and `collMod`

## Step 1: Generate both output scripts

Run from the `indexes` folder while connected to the source cluster:

```bash
mongosh "<SOURCE_CONNECTION_STRING>" --file createIndexDefinition.js
```

This produces:

1. `createIndexesAtDestination.js`
2. `postCutoverRectifyIndexes.js`

## Step 2: Run pre-cutover index creation on destination

Run against the destination cluster before cutover:

```bash
mongosh "<DESTINATION_CONNECTION_STRING>" --file createIndexesAtDestination.js
```

## Step 3: Run post-cutover rectification on destination

After cutover, run:

```bash
mongosh "<DESTINATION_CONNECTION_STRING>" --file postCutoverRectifyIndexes.js
```

This re-enables uniqueness and restores original TTL values.

## Recommended validation

After each step, validate index state on critical collections:

```javascript
db.getSiblingDB("<dbName>").getCollection("<collectionName>").getIndexes()
```

## Notes

1. The generator skips the `_id_` index.
2. If duplicate data exists post-cutover, `prepareUnique/unique` may fail for affected indexes. Resolve duplicates and rerun the post-cutover script.
