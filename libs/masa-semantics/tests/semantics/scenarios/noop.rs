//! Builds with no scheduling feature: the hooks do nothing.

use crate::harness::*;

scenario! {
    /// Masa adds nothing to a call graph when no scheduling feature is enabled:
    /// child requests carry no Masa context, replies carry none, and handlers
    /// run as written.
    fn hooks_add_nothing(w) {
        let a = w.service("NoopA");
        let b = w.service("NoopB");
        let req = w.ingress("NoopApi", dur_ms(10));
        let ran = std::cell::Cell::new(false);
        let reply = a.serve("Entry", &req, |ha| {
            let out = ha.call(&b, "Next").outbound_or_untouched();
            assert!(out.is_ok());
            assert!(!out.unwrap().carries_context());
            ran.set(true);
            ha.work_ms(50)
        });
        assert!(ran.get());
        assert!(reply.is_ok());
        assert!(reply.view().is_none());
    }
}
