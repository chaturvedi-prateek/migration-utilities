/**
 * Script: createIndexScript.js
 * 
 * Description: This script extracts indexes from a MongoDB database and generates a script to create those indexes at destination.
 */
//Libraries required in mongosh
const fs = require('fs');

// Variable definition
const filePath = 'createIndexesAtDestination.js';

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
 * Extracts indexes from a database and generates a script to create those indexes.
 *
 * @param {object} db - The database object.
 * @returns {string} - The script to create the indexes in text format.
 */
function extractIndexes(db) {
    let textToWrite = '';
    var i = 0;
    db.getCollectionInfos().forEach((c) => {
        if (c.type !== "collection" || c.name === "system.views") return;

        const collection = db.getCollection(c.name);

        textToWrite += "\n//Database: " + db.getName() +  " Collection: " + c.name + " (" + ++i + ")\n";

        // [options, indexSpec]
        const group = new Map();

        collection.getIndexes().forEach(({ v, key, name, ns, ...options }) => {
            const optionsKey = JSON.stringify(options);
            const listIndexes = group.get(optionsKey) || group.set(optionsKey, []).get(optionsKey);

            listIndexes.push(key);
        })

        group.forEach((listIndexes, options) => {
            textToWrite += "db.getSiblingDB('" + db.getName() + "').getCollection('" + c.name + "').createIndexes([";

            listIndexes.forEach(indexSpec => {
                textToWrite += "\t" + JSON.stringify(indexSpec) + ",";
            });

            if (options !== "{}") {
                textToWrite += "\t],";
                textToWrite += "\t" + options;
                textToWrite += ");";
            } else {
                textToWrite += "]);";
            }
        });
    });
    return textToWrite;
}

/**
 * Iterates through all databases and extracts indexes.
 * 
 * @returns {void}
 */
function iterateDatabases() {
    let scriptText = '';
    const databases = db.adminCommand({ listDatabases: 1 }).databases;

    databases.forEach(database => {
        const dbName = database.name;
        const dbToUse = db.getSiblingDB(dbName);
        scriptText += extractIndexes(dbToUse);
        
    });
    writeTextToFile(filePath, scriptText);
}

iterateDatabases();