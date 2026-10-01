/// Runtime task priority: the name under which `Meta` is exposed to code that
/// treats it as a priority number.
pub type TaskPriority = super::meta::Meta;

pub(crate) trait TaskPrioritize {
    fn priority(&self) -> super::meta::Meta;
}
