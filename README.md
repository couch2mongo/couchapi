# CouchAPI

Why not pretend to be CouchDB (or Cloudant) and talk to MongoDB?

**WARNING: THERE ARE STILL MANY DRAGONS**

This should all work and I have tested it. It's fast - you should try it!

## Requirements

You need both Rust Stable and Nightly!

```bash
rustup update stable
rustup toolchain install nightly
```

## Building

```bash
cargo build
```

## Running

```bash
cargo run -- --help
```

See `config.toml` for an example configuration file.

## Usage

**Warning**: We don't support 'views' yet. Only basic CRUD operations are supported.

Whenever you see a dbname, it means a collection.

### Get a document

```bash
curl -X GET http://localhost:5984/dbname/docid
```

### Create a document

```bash
curl -X POST http://localhost:5984/dbname -d '{"_id": "docid", "foo": "bar"}'
```

OR

```bash
curl -X PUT http://localhost:5984/dbname/docid -d '{"foo": "bar"}'
```

### Update a document

```bash
curl -X PUT http://localhost:5984/dbname/docid -d '{"_id": "docid", "_rev": "1-1234", "foo": "baz"}'
```

OR

```bash
curl -X PUT http://localhost:5984/dbname/docid -d '{"foo": "baz"}' -H 'If-Match: 1-1234'
```

OR

```bash
curl -X POST http://localhost:5984/dbname -d '{"_id": "docid", "_rev": "1-1234", "foo": "baz"}'
```

### Delete a document

```bash
curl -X DELETE http://localhost:5984/dbname/docid?rev=1-1234
```