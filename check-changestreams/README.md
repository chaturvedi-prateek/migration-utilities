# check-changestreams

A small Go CLI that inspects an **Amazon DocumentDB** (or MongoDB) cluster and
reports, for every collection, whether a **change stream** can be opened.

## Why a functional check?

DocumentDB does not expose a reliable command to list which collections have
change streams enabled. So instead of trusting an internal metadata collection,
this tool uses the dependable signal: it **tries to open a change stream** on
each collection.

| Result | Meaning |
| --- | --- |
| `ENABLED` | A change stream opened successfully. |
| `DISABLED` | DocumentDB rejected it as "change stream not enabled / disabled". |
| `UNKNOWN` | Some other error occurred, so it is **not** treated as disabled. |

## Requirements

- Go 1.21+
- Network access to the cluster (cluster endpoint)
- The RDS CA bundle (`global-bundle.pem`) if TLS is enabled (the DocumentDB default)
- A user with permission to list databases/collections and open change streams

Download the CA bundle:

```bash
curl -o global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

## Install / Build

```bash
go mod tidy
go build -o check-changestreams .
```

## Usage

Set the connection string via `-uri` or the `DOCDB_URI` environment variable:

```bash
export DOCDB_URI="mongodb://user:pass@your-cluster:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred"
```

Then run:

```bash
# Table output, all non-system databases
./check-changestreams -ca-file global-bundle.pem

# Limit to a single database
./check-changestreams -ca-file global-bundle.pem -db mydb

# Machine-readable JSON
./check-changestreams -ca-file global-bundle.pem -json
```

Or without building:

```bash
go run . -ca-file global-bundle.pem
```

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-uri` | `$DOCDB_URI` | Connection string. |
| `-ca-file` | _(none)_ | Path to the CA bundle (e.g. `global-bundle.pem`) for DocumentDB TLS. |
| `-db` | _(all)_ | Restrict the check to a single database. |
| `-json` | `false` | Emit results as JSON instead of a table. |
| `-timeout` | `30s` | Overall operation timeout. |

## Output

Table:

```
DATABASE                  COLLECTION                     STATUS     DETAIL
------------------------------------------------------------------------------------------
mydb                      orders                         ENABLED
mydb                      audit_log                      DISABLED
mydb                      legacy                          UNKNOWN    <error message>
```

JSON:

```json
[
  { "database": "mydb", "collection": "orders", "status": "ENABLED" },
  { "database": "mydb", "collection": "audit_log", "status": "DISABLED" }
]
```

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | All checked collections are `ENABLED`. |
| `3` | At least one collection is not confirmed enabled (`DISABLED` or `UNKNOWN`). |
| `1` | Connection / runtime error. |
| `2` | Missing/invalid configuration (e.g. no connection string). |

These make the tool easy to drop into CI or a monitoring job.

## Notes

- The `DISABLED` detection matches on DocumentDB's error **message text**,
  because DocumentDB does not return a stable dedicated error code for this
  condition. If your engine version returns a different message, adjust
  `isChangeStreamNotEnabled` in `main.go`.
- System databases (`admin`, `config`, `local`) are skipped.
- Change streams are served from the primary; connect via the cluster endpoint.
- Enable a change stream with the admin command:

  ```javascript
  db.adminCommand({
    modifyChangeStreams: 1,
    database: "mydb",
    collection: "mycollection",
    enable: true
  });
  ```
