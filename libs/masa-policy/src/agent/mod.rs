// Agent-owned policy stack, selected by the `stack_custom` feature.
//
// This directory is the one place a new policy is written without touching
// Masa's built-in modules or `masa_stack.rs`: add modules as files here and
// set `AgentStack` to the stack that should run. With `stack_custom` enabled,
// `masa::DefaultHooks` is `PolicyHooks<AgentStack>`, so every generated server
// and client stub in the binary uses this stack. See docs/POLICY_MODULES.md.
//
// It starts as `MasaStack`, so `<features>,stack_custom` behaves exactly like
// `<features>` until this alias changes.

/// The policy stack run when the `stack_custom` feature is enabled.
pub type AgentStack = crate::MasaStack;
