# CouchAPI

A shift from Apache CouchDB to MongoDB addresses the need for superior
scalability, flexibility, and efficiency. With the innovative use of a proxy
layer developed in Rust, a modern systems programming language, this migration
can be significantly simplified, making the transition seamless and efficient.

Green Man Gaming uses this in production, serving many transactions per second
and without it, our store would end up offline. We're making this public so
others can get as much use from it as we have!

## Requirements

You need Rust Stable and Nightly (we only use Nightly for rustfmt)!

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

## Pro-tips for development

If you get a random error about `traits` add `#[debug_handler]` to
the relevant handler, and you'll get actual information. Please don't 
commit code with this handler!

## Usage

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
