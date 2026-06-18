/**
 * Script: createIndexDefinition.js
 *
 * Run on the SOURCE (DocDB) with mongosh. Reads all indexes across all non-system
 * databases and writes two output scripts to the current directory:
 *
 *   createIndexesAtDestination.js  — run on Atlas BEFORE cutover
 *     • TTL indexes get expireAfterSeconds = MAX_INT (deactivated during migration)
 *     • Unique indexes are created as non-unique (avoids uniqueness violations during sync)
 *     • All createIndex calls for a collection run in parallel via Promise.all
 *
 *   postCutoverRectifyIndexes.js   — run on Atlas AFTER cutover
 *     • Phase 1 (parallel): restore original TTL values + prepareUnique on all formerly-unique indexes
 *     • Phase 2 (parallel): convert all prepareUnique indexes to unique:true
 */
const fs = require('fs');

const createIndexesFilePath = 'createIndexesAtDestination.js';
const postCutoverFilePath = 'postCutoverRectifyIndexes.js';
const MAX_TTL_SECONDS = 2147483647;
const BATCH_SIZE = 10; // max concurrent operations per Promise.all block

function writeTextToFile(filePath, textToWrite) {
    fs.writeFile(filePath, textToWrite, (err) => {
        if (err) {
            console.error('Error writing to file:', err);
        } else {
            console.log(`Written: ${filePath}`);
        }
    });
}

/**
 * Splits an array of statement strings into batched `await Promise.all([...])` blocks.
 */
function buildBatchedBlocks(statements) {
    let out = '';
    for (let i = 0; i < statements.length; i += BATCH_SIZE) {
        const batch = statements.slice(i, i + BATCH_SIZE);
        out += '  await Promise.all([\n';
        out += batch.map(s => '    ' + s).join(',\n');
        out += '\n  ]);\n';
    }
    return out;
}

/**
 * Visits every non-system collection in `dbObj` and appends to the three accumulator arrays:
 *   perCollCreate   — { label, statements[] } one entry per collection (for createIndex batching)
 *   phase1Ops       — flat list of async-IIFE strings: prepareUnique + TTL restores
 *   phase2Ops       — flat list of async-IIFE strings: unique:true finalisation
 */
function collectIndexes(dbObj, perCollCreate, phase1Ops, phase2Ops, counter) {
    const dbName = dbObj.getName();

    dbObj.getCollectionInfos().forEach((c) => {
        if (c.type !== 'collection' || c.name === 'system.views') return;

        const collection = dbObj.getCollection(c.name);
        const createStatements = [];

        collection.getIndexes().forEach(({ v, key, name, ns, ...options }) => {
            if (name === '_id_') return;

            const originalOptions = { ...options };
            const migrationOptions = { ...options };

            originalOptions.name = name;
            migrationOptions.name = name;

            const hasTtl = Object.prototype.hasOwnProperty.call(migrationOptions, 'expireAfterSeconds');
            const isUnique = migrationOptions.unique === true;

            if (hasTtl) {
                migrationOptions.expireAfterSeconds = MAX_TTL_SECONDS;
            }
            if (isUnique) {
                migrationOptions.unique = false;
            }

            createStatements.push(
                `db.getSiblingDB('${dbName}').getCollection('${c.name}').createIndex(${JSON.stringify(key)}, ${JSON.stringify(migrationOptions)})`
            );

            if (isUnique) {
                phase1Ops.push(
                    `(async () => { try { await db.getSiblingDB('${dbName}').runCommand({ collMod: '${c.name}', index: { name: '${name}', prepareUnique: true } }); print('prepareUnique ok: ${dbName}.${c.name}.${name}'); } catch (e) { print('prepareUnique FAILED: ${dbName}.${c.name}.${name} -> ' + e.message); } })()`
                );
                phase2Ops.push(
                    `(async () => { try { await db.getSiblingDB('${dbName}').runCommand({ collMod: '${c.name}', index: { name: '${name}', unique: true } }); print('unique ok: ${dbName}.${c.name}.${name}'); } catch (e) { print('unique FAILED: ${dbName}.${c.name}.${name} -> ' + e.message); } })()`
                );
            }

            if (hasTtl) {
                phase1Ops.push(
                    `(async () => { try { await db.getSiblingDB('${dbName}').runCommand({ collMod: '${c.name}', index: { name: '${name}', expireAfterSeconds: ${originalOptions.expireAfterSeconds} } }); print('TTL restored: ${dbName}.${c.name}.${name} -> ${originalOptions.expireAfterSeconds}s'); } catch (e) { print('TTL restore FAILED: ${dbName}.${c.name}.${name} -> ' + e.message); } })()`
                );
            }
        });

        if (createStatements.length > 0) {
            perCollCreate.push({
                label: `${dbName}.${c.name} (${++counter[0]})`,
                statements: createStatements,
            });
        }
    });
}

function iterateDatabases() {
    const SYSTEM_DBS = new Set(['admin', 'local', 'config']);
    const databases = db.adminCommand({ listDatabases: 1 }).databases;

    const perCollCreate = [];
    const phase1Ops = [];
    const phase2Ops = [];
    const counter = [0];

    databases.forEach(({ name: dbName }) => {
        if (SYSTEM_DBS.has(dbName)) return;
        collectIndexes(db.getSiblingDB(dbName), perCollCreate, phase1Ops, phase2Ops, counter);
    });

    // ── createIndexesAtDestination.js ────────────────────────────────────────
    let createScript = '(async () => {\n';
    for (const { label, statements } of perCollCreate) {
        createScript += `\n  // ${label}\n`;
        createScript += buildBatchedBlocks(statements);
    }
    createScript += '\n})();\n';

    // ── postCutoverRectifyIndexes.js ─────────────────────────────────────────
    let rectifyScript = '(async () => {\n';

    if (phase1Ops.length > 0) {
        rectifyScript += '\n  // Phase 1: TTL restores + prepareUnique (all in parallel)\n';
        rectifyScript += buildBatchedBlocks(phase1Ops);
    }

    if (phase2Ops.length > 0) {
        rectifyScript += '\n  // Phase 2: finalise unique indexes (runs after Phase 1 completes)\n';
        rectifyScript += buildBatchedBlocks(phase2Ops);
    }

    rectifyScript += '\n})();\n';

    writeTextToFile(createIndexesFilePath, createScript);
    writeTextToFile(postCutoverFilePath, rectifyScript);
}

iterateDatabases();
