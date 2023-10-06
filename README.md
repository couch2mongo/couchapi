# CouchAPI

A shift from Apache CouchDB to MongoDB addresses the need for superior
scalability, flexibility, and efficiency. With the innovative use of a proxy
layer developed in Rust, a modern systems programming language, this migration
can be significantly simplified, making the transition seamless and efficient.

## Proxy Approach and Rust: A Perfect Symbiosis

A proxy layer serves as an intermediary, translating CouchDB API calls and
queries into MongoDB's syntax. Implementing this layer in Rust brings the
adds advantages of Rust's strong performance, reliability,
and its ‘zero-cost abstraction’ principle, which ensures that abstractions
do not incur runtime costs.

## Benefits of the Rust-Based Proxy Approach

1. **Efficient Migration**: With Rust's emphasis on performance and memory
   safety, the proxy layer can handle high loads and complex translations with
   efficiency and without crashes or security vulnerabilities, ensuring a
   smooth migration process.
2. **Code Preservation**: By using the Rust-based proxy, existing application
   code remains largely untouched. This significantly reduces the time, cost,
   and potential errors associated with substantial code rewrites, making the
   migration process more manageable.
3. **Error Handling**: Rust’s rich type system and its "panic" mechanism allow
   the proxy to catch and handle errors gracefully, reducing the likelihood
   of runtime errors during the migration.

## Requirements

You need both Rust Stable and Nightly (we only use Nightly for rustfmt)!

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
the relevant handler and you'll get actual information. Do not commit code
with this handler!

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
