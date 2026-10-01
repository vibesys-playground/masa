# rpcstack-wire

The layout of the header that carries a message's module wire data, and the
primitives to read and write it. A leaf crate: it depends only on `serde`,
`serde_json` and `base64`, so a crate that cannot depend on tonic or http can
still read one section.

```text
ctx: <name> : <base64 JSON> [ . <name> : <base64 JSON> ]*
```

Each section is one module's wire data. Neither `.` nor `:` is in the base64
alphabet and section names may not contain them, so the value is split with
plain string searches and one section is found without decoding any other.

Public surface:

- `HEADER_NAME`: the header's name, `ctx`.
- `SECTION_SEPARATOR`, `NAME_SEPARATOR`.
- `sections(header)`: the undecoded `(name, payload)` pairs.
- `find_section(header, name)`: one undecoded payload.
- `decode_payload::<W>(payload)` and `encode_payload(&wire)`: base64 JSON, with
  an error that says which step failed.
- `push_section(&mut header, name, payload)`.

`rpcstack` builds the typed module API (`WireIn`, `WireOut`) on these.
