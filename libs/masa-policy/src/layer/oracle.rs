// Oracle layer — perfect-information child priority assignment.
//
// Included in the default stack only with `sched_oracle`. The layer consumes synthetic-provided
// metadata describing the child RPC's wall-clock work and the remaining
// wall-clock work after that child returns. It then converts that information
// into a child completion deadline and priority hint.

use masa_core::{
    Context, PriorityHint, ORACLE_CHILD_WORK_US_HEADER, ORACLE_REMAINING_AFTER_US_HEADER,
};
use tonic::{CowGrpcMethod, Request, Status};

use super::{ChildRpcContext, Layer, LayerChild, LayerServer, ServerInit};

#[derive(Debug)]
pub struct OracleServer;

impl LayerServer for OracleServer {
    fn new(_init: &mut ServerInit) -> Self {
        Self
    }
}

#[derive(Debug)]
pub struct OracleLayer;

impl Layer for OracleLayer {
    type Server = OracleServer;
    type Child = OracleChild;

    fn new(_method: &CowGrpcMethod, _server: &OracleServer, _ctx: &mut Context) -> Self {
        Self
    }

    fn before_child_rpc<T>(
        &self,
        ctx: &Context,
        child_method: &CowGrpcMethod,
        _child_ctx: &mut OracleChild,
        request: &mut Request<T>,
        child_rpc: &mut ChildRpcContext,
    ) -> Result<(), Status> {
        let hint = OracleHint::from_request(request, child_method)?;
        let completion_deadline = ctx.e2e_deadline().saturating_sub(hint.remaining_after_us);
        let latest_start = completion_deadline.saturating_sub(hint.child_work_us);

        child_rpc.deadline = completion_deadline;
        child_rpc.prio_hint = PriorityHint::new(latest_start);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OracleChild;

impl LayerChild for OracleChild {
    fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OracleHint {
    child_work_us: u64,
    remaining_after_us: u64,
}

impl OracleHint {
    fn from_request<T>(request: &Request<T>, child_method: &CowGrpcMethod) -> Result<Self, Status> {
        let metadata = request.metadata();
        let child_work_us = read_u64_header(metadata, ORACLE_CHILD_WORK_US_HEADER, child_method)?;
        let remaining_after_us =
            read_u64_header(metadata, ORACLE_REMAINING_AFTER_US_HEADER, child_method)?;

        Ok(Self {
            child_work_us,
            remaining_after_us,
        })
    }
}

fn read_u64_header(
    metadata: &tonic::metadata::MetadataMap,
    key: &'static str,
    child_method: &CowGrpcMethod,
) -> Result<u64, Status> {
    let value = metadata.get(key).ok_or_else(|| {
        Status::internal(format!(
            "missing oracle header '{}' for {}::{}",
            key,
            child_method.service(),
            child_method.method()
        ))
    })?;

    let s = value.to_str().map_err(|e| {
        Status::internal(format!(
            "invalid oracle header '{}' for {}::{}: {}",
            key,
            child_method.service(),
            child_method.method(),
            e
        ))
    })?;

    s.parse::<u64>().map_err(|e| {
        Status::internal(format!(
            "invalid oracle header '{}' value '{}' for {}::{}: {}",
            key,
            s,
            child_method.service(),
            child_method.method(),
            e
        ))
    })
}
