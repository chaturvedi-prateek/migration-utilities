/**
 * Script: createIndexScript.js
 * 
 * Description: This script extracts indexes from a MongoDB database and generates a script to create those indexes at destination.
 */
//Libraries required in mongosh
const fs = require('fs');

// Variable definition
const createIndexesFilePath = 'createIndexesAtDestination.js';
const postCutoverFilePath = 'postCutoverRectifyIndexes.js';
const MAX_TTL_SECONDS = 2147483647;

// Functions
/**
 * Writes the given text to a file at the specified file path.
 * 
 * @param {string} filePath - The path of the file to write to.
 * @param {string} textToWrite - The text to write to the file.
 * @returns {void}
 */
function writeTextToFile(filePath, textToWrite) {
    fs.writeFile(filePath, textToWrite, (err) => {
        if (err) {
            console.error('Error writing to file:', err);
        } else {
            console.log('Text written to file successfully!');
        }
    });
}

/**
 * Extracts indexes from a database and generates scripts for pre-cutover and post-cutover index handling.
 *
 * @param {object} db - The database object.
 * @returns {{createScriptText: string, rectifyScriptText: string}} - Script contents for both phases.
 */
function extractIndexes(db) {
    let createScriptText = '';
    let rectifyScriptText = '';
    var i = 0;
    db.getCollectionInfos().forEach((c) => {
        if (c.type !== "collection" || c.name === "system.views") return;

        const collection = db.getCollection(c.name);

        createScriptText += "\n//Database: " + db.getName() +  " Collection: " + c.name + " (" + ++i + ")\n";
        rectifyScriptText += "\n//Database: " + db.getName() +  " Collection: " + c.name + " (" + i + ")\n";

        collection.getIndexes().forEach(({ v, key, name, ns, ...options }) => {
            if (name === "_id_") return;

            const originalOptions = { ...options };
            const migrationOptions = { ...options };

            // Preserve source index names so post-cutover collMod can target them reliably.
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

            createScriptText += "db.getSiblingDB('" + db.getName() + "').getCollection('" + c.name + "').createIndex(" + JSON.stringify(key) + ", " + JSON.stringify(migrationOptions) + ");\n";

            if (isUnique) {
                rectifyScriptText += "try {\n";
                rectifyScriptText += "  db.getSiblingDB('" + db.getName() + "').runCommand({ collMod: '" + c.name + "', index: { name: '" + name + "', prepareUnique: true } });\n";
                rectifyScriptText += "  db.getSiblingDB('" + db.getName() + "').runCommand({ collMod: '" + c.name + "', index: { name: '" + name + "', unique: true } });\n";
                rectifyScriptText += "  print('Rectified unique index via collMod: " + db.getName() + "." + c.name + "." + name + "');\n";
                rectifyScriptText += "} catch (e) {\n";
                rectifyScriptText += "  print('Failed to rectify unique index via collMod: " + db.getName() + "." + c.name + "." + name + " -> ' + e.message);\n";
                rectifyScriptText += "}\n";
            }

            if (hasTtl) {
                rectifyScriptText += "try {\n";
                rectifyScriptText += "  db.getSiblingDB('" + db.getName() + "').runCommand({ collMod: '" + c.name + "', index: { name: '" + name + "', expireAfterSeconds: " + originalOptions.expireAfterSeconds + " } });\n";
                rectifyScriptText += "  print('Rectified TTL index via collMod: " + db.getName() + "." + c.name + "." + name + " to " + originalOptions.expireAfterSeconds + " seconds');\n";
                rectifyScriptText += "} catch (e) {\n";
                rectifyScriptText += "  print('Failed to rectify TTL index via collMod: " + db.getName() + "." + c.name + "." + name + " -> ' + e.message);\n";
                rectifyScriptText += "}\n";
            }
        });
    });
    return {
        createScriptText,
        rectifyScriptText
    };
}

/**
 * Iterates through all databases and extracts indexes.
 * 
 * @returns {void}
 */
function iterateDatabases() {
    let createScriptText = '';
    let rectifyScriptText = '';
    const SYSTEM_DBS = new Set(['admin', 'local', 'config']);
    const databases = db.adminCommand({ listDatabases: 1 }).databases;

    databases.forEach(database => {
        const dbName = database.name;
        if (SYSTEM_DBS.has(dbName)) return;
        const dbToUse = db.getSiblingDB(dbName);
        const scriptTexts = extractIndexes(dbToUse);
        createScriptText += scriptTexts.createScriptText;
        rectifyScriptText += scriptTexts.rectifyScriptText;
        
    });
    writeTextToFile(createIndexesFilePath, createScriptText);
    writeTextToFile(postCutoverFilePath, rectifyScriptText);
}

iterateDatabases();