// Per-RPC cost of the estimation hooks. Ignored by default because timings are
// only meaningful in release mode:
//
//   cargo test --release -p masa-policy \
//       --features sched_pred,abort_slack,ac_pred,est_mean_var \
//       --test estimation_hook_bench -- --ignored --nocapture
//
// One iteration is the hook work of a handler that issues one child RPC:
// begin + before_child_rpc + after_child_rpc + finalize.

#![cfg(feature = "estimator")]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use masa_core::{time_now, ContextBuilder};
use masa_policy::modules::EstimationLayer;
use masa_policy::{
    EstimationResponseWire, EstimationWire, MasaResponseExt, PolicyHooks, MASA_CONTEXT_HEADER,
};
use tonic::masa::{ClientHooks, Hooks, ParentHooks, ServerHooks};
use tonic::{GrpcMethod, Request, Response, Status};

type Srv = <PolicyHooks as Hooks>::ServerContext;
type Par = <PolicyHooks as Hooks>::ParentContext;
type Chi = <PolicyHooks as Hooks>::ChildContext;

/// A successful child response carrying the estimation data a real child
/// attaches.
fn child_response(parent: &masa_core::Context) -> Response<()> {
    let mut response = Response::new(()).with_masa_context(parent);
    response.set_wire::<EstimationLayer>(&EstimationWire::response(EstimationResponseWire {
        compute_time_us: 4_000,
        accumulated_compute_us: 5_000,
        utilization: 0.5,
        max_downstream_util: 0.5,
        early_return_count: 0,
        deadline_signal_count: 0,
    }));
    response
}

#[test]
#[ignore = "timing; run with --release -- --ignored --nocapture"]
fn per_rpc_hook_cost() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        tokio::spawn(async {
            let server = Arc::new(Srv::new("BenchSvc"));
            let now = time_now();
            let ctx = ContextBuilder::new("BenchSvc", 1)
                .slo(100_000_000_000)
                .gateway_entry(now)
                .deadline(now + 100_000_000_000)
                .build();
            let req = http::Request::builder()
                .header(MASA_CONTEXT_HEADER, ctx.to_header_string())
                .body(())
                .unwrap();
            let parent = GrpcMethod::new("BenchSvc", "Parent");
            let child = GrpcMethod::new("BenchSvc", "Child");
            let template = child_response(&ctx);
            let rebuild = || {
                let mut response = Response::new(());
                *response.metadata_mut() = template.metadata().clone();
                response
            };

            let iterations = 200_000u32;
            let run = |with_hooks: bool| {
                let start = Instant::now();
                for _ in 0..iterations {
                    let response = rebuild();
                    if !with_hooks {
                        black_box(response);
                        continue;
                    }
                    let p = Par::begin(parent, &req, server.clone());
                    let mut creq = Request::new(());
                    let mut cc = Chi::new(child, &creq);
                    p.before_child_rpc(child, &mut creq, &mut cc).unwrap();
                    let mut response = Ok(response);
                    p.after_child_rpc(child, &mut response, cc).unwrap();
                    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
                    p.finalize_before_serialization(&mut result);
                    black_box((creq, result.is_ok()));
                }
                start.elapsed().as_nanos() as f64 / f64::from(iterations)
            };
            let phases = || {
                let mut totals = [0u128; 4];
                for _ in 0..iterations {
                    let response = rebuild();
                    let t0 = Instant::now();
                    let p = Par::begin(parent, &req, server.clone());
                    let t1 = Instant::now();
                    let mut creq = Request::new(());
                    let mut cc = Chi::new(child, &creq);
                    p.before_child_rpc(child, &mut creq, &mut cc).unwrap();
                    let t2 = Instant::now();
                    let mut response = Ok(response);
                    p.after_child_rpc(child, &mut response, cc).unwrap();
                    let t3 = Instant::now();
                    let mut result: Result<Response<()>, Status> = Ok(Response::new(()));
                    p.finalize_before_serialization(&mut result);
                    let t4 = Instant::now();
                    black_box((creq, result.is_ok()));
                    for (total, (a, b)) in totals.iter_mut().zip([(t0, t1), (t1, t2), (t2, t3), (t3, t4)]) {
                        *total += b.duration_since(a).as_nanos();
                    }
                }
                totals.map(|t| t as f64 / f64::from(iterations))
            };
            phases();
            let [begin, before_child, after_child, finalize] = phases();
            println!(
                "phases (ns): begin {begin:.0}, before_child_rpc {before_child:.0}, \
                 after_child_rpc {after_child:.0}, finalize {finalize:.0}"
            );
            run(true);
            let base = run(false);
            let mut samples: Vec<f64> = (0..5).map(|_| run(true)).collect();
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "per-RPC hooks: median {:.0} ns (min {:.0}, max {:.0}); response rebuild alone {:.0} ns",
                samples[2], samples[0], samples[4], base
            );
        })
        .await
        .unwrap();
    });
}
