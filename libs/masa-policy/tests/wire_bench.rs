// Timed comparison of reading a `ctx` header four ways. Ignored by default
// because timings are only meaningful in release mode:
//
//   cargo test --release -p masa-policy --test wire_bench -- --ignored --nocapture
//
// (a) decode the whole budget `Context`,
// (a2) read only its priority (what hyper does per HTTP/2 stream),
// (b) split the wire envelope into sections without decoding any,
// (c) `peek` one small module section.

use std::hint::black_box;
use std::time::Instant;

use masa_core::{time_now, PriorityHint};
use masa_policy::ContextBuilder;
use masa_policy::{peek, Extensions, MasaRequestExt, Module, WireIn, WireOut, MASA_CONTEXT_HEADER};
use serde::{Deserialize, Serialize};
use tonic::{CowGrpcMethod, Request};

#[derive(Debug, Serialize, Deserialize)]
struct Small {
    tokens: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Larger {
    label: String,
    values: Vec<u64>,
}

macro_rules! bench_module {
    ($name:ident, $key:literal, $wire:ty) => {
        #[derive(Debug)]
        struct $name;

        impl Module for $name {
            type Server = ();
            const NAME: &'static str = $key;
            type Wire = $wire;

            fn new(_m: &CowGrpcMethod, _s: &(), _w: &WireIn<'_>, _ext: &mut Extensions) -> Self {
                Self
            }
        }
    };
}

bench_module!(SmallModule, "small", Small);
bench_module!(LargerModule, "larger", Larger);
bench_module!(OtherModule, "other", Small);

fn time(label: &str, iterations: u32, mut f: impl FnMut()) {
    for _ in 0..iterations / 10 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / f64::from(iterations);
    println!("{label:<56} {ns:>9.1} ns/op");
}

#[test]
#[ignore = "timing; run with --release -- --ignored --nocapture"]
fn compare_context_decode_envelope_split_and_peek() {
    let now = time_now();
    let ctx = ContextBuilder::new("hotel.SearchHotels", 42)
        .slo(500_000)
        .gateway_entry(now)
        .deadline(now + 500_000)
        .prio_hint(PriorityHint::new(now + 500_000))
        .build();

    let mut request = Request::new(());
    let mut out = WireOut::new();
    out.put::<SmallModule>(&Small { tokens: 40 }).unwrap();
    out.put::<LargerModule>(&Larger {
        label: "a moderately long label for a payload".into(),
        values: (0..16).collect(),
    })
    .unwrap();
    out.put::<OtherModule>(&Small { tokens: 7 }).unwrap();
    out.install(request.metadata_mut());
    request.set_masa_context(&ctx);

    let value = request
        .metadata()
        .get(MASA_CONTEXT_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    println!(
        "ctx header: {} bytes, budget section alone {} bytes",
        value.len(),
        ctx.to_header_string().len()
    );

    let mut headers = http::HeaderMap::new();
    headers.insert(MASA_CONTEXT_HEADER, value.parse().unwrap());

    let mut plain = http::HeaderMap::new();
    plain.insert(MASA_CONTEXT_HEADER, ctx.to_header_string().parse().unwrap());

    let n = 1_000_000;
    time("(a) full Context decode, budget section only", n, || {
        black_box(masa_core::read_context_from_headers(black_box(&plain)));
    });
    time("(a) full Context decode, header with 4 sections", n, || {
        black_box(masa_core::read_context_from_headers(black_box(&headers)));
    });
    time("(a2) priority only, header without wire data", n, || {
        black_box(masa_core::read_priority_from_headers(black_box(&plain)));
    });
    time("(a2) priority only, header with 4 sections", n, || {
        black_box(masa_core::read_priority_from_headers(black_box(&headers)));
    });
    time("(b) envelope split (WireIn::from_headers)", n, || {
        black_box(WireIn::from_headers(black_box(&headers)).unwrap());
    });
    time("(c) peek first section, 8-byte payload", n, || {
        black_box(peek::<SmallModule>(black_box(&headers)).unwrap());
    });
    time("(c) peek last section, 8-byte payload", n, || {
        black_box(peek::<OtherModule>(black_box(&headers)).unwrap());
    });
    time("(c) peek middle section, larger payload", n, || {
        black_box(peek::<LargerModule>(black_box(&headers)).unwrap());
    });
}
