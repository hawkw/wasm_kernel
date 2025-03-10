//! Shared defs between the kernel and the test runner.
//!
//! This is in its own crate mainly so the constants are the same and I can't
//! have the kernel write the wrong strings (which I did lol).

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

use core::{
    cmp,
    fmt::{self, Write},
    marker::PhantomData,
};
pub mod assert;
pub mod report;
#[cfg(feature = "runner")]
pub mod runner;

pub type TestResult = Result<(), assert::Failed>;
pub type Outcome = Result<(), Failure>;
pub use report::Failure;

#[derive(Clone, Eq, PartialEq)]
pub struct TestName<'a, S = &'a str> {
    name: S,
    module: S,
    _lt: PhantomData<&'a str>,
}

/// Test descriptor created by `decl_test!`. Describes and allows running an
/// individual test.
pub struct Test {
    #[doc(hidden)]
    pub descr: TestName<'static>,
    #[doc(hidden)]
    pub run: fn() -> Outcome,
}

/// Type which may be used as a test return type.
pub trait TestReport {
    /// Report any errors to `tracing`, then returns either `true` for a
    /// success, or `false` for a failure.
    fn report(self) -> Outcome;
}

impl TestReport for () {
    fn report(self) -> Outcome {
        Ok(())
    }
}

impl<T: fmt::Debug> TestReport for Result<(), T> {
    fn report(self) -> Outcome {
        match self {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::error!("FAIL {:?}", err);
                Err(Failure::Fail)
            }
        }
    }
}

/// Declare a new test, sort-of like the `#[test]` attribute.
// FIXME: Declare a `#[test]` custom attribute instead?
#[macro_export]
macro_rules! decl_test {
    (fn $name:ident $($t:tt)*) => {
        // Clippy will complain about these functions returning `Result<(),
        // ()>` unnecessarily, but this is actually required by the test
        // harness.
        #[allow(clippy::unnecessary_wraps)]
        fn $name $($t)*

        // Introduce an anonymous const to avoid name conflicts. The `#[used]`
        // will prevent the symbol from being dropped, and `link_section` will
        // make it visible.
        const _: () = {
            #[used]
            #[cfg_attr(any(target_os = "macos", target_os = "ios"), link_section = "Mycelium,Tests")]
            #[cfg_attr(not(any(target_os = "macos", target_os = "ios")), link_section = "MyceliumTests")]
            static TEST: $crate::Test = $crate::Test {
                descr: $crate::TestName::new(module_path!(), stringify!($name)),
                run: || $crate::TestReport::report($name()),
            };
        };
    }
}

// === impl TestName ===

impl<S> TestName<'_, S> {
    pub const fn new(module: S, name: S) -> Self {
        Self {
            name,
            module,
            _lt: PhantomData,
        }
    }
}

impl<S> TestName<'_, S>
where
    S: AsRef<str>,
{
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn module(&self) -> &str {
        self.module.as_ref()
    }
}

impl<S: Ord> cmp::PartialOrd for TestName<'_, S> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Ord> cmp::Ord for TestName<'_, S> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.module
            .cmp(&other.module)
            .then_with(|| self.name.cmp(&other.name))
    }
}

// Custom impl to skip `PhantomData` field.
impl<S: fmt::Debug> fmt::Debug for TestName<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { name, module, _lt } = self;
        f.debug_struct("Test")
            .field("name", name)
            .field("module", module)
            .finish()
    }
}

impl<S: fmt::Display> fmt::Display for TestName<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { name, module, _lt } = self;
        write!(f, "{module}::{name}")
    }
}

// === impl Test ===

impl Test {
    pub fn write_outcome(&self, outcome: Outcome, mut writer: impl Write) -> fmt::Result {
        tracing::trace!(?self.descr, ?outcome, "write_outcome",);
        match outcome {
            Ok(()) => writeln!(
                writer,
                "{} {} {}",
                report::PASS_TEST,
                self.descr.module,
                self.descr.name
            ),
            Err(fail) => writeln!(
                writer,
                "{} {} {} {}",
                report::FAIL_TEST,
                fail.as_str(),
                self.descr.module,
                self.descr.name
            ),
        }
    }
}

decl_test! {
    fn it_works() -> Result<(), ()> {
        tracing::info!("I'm running in a test!");
        Ok(())
    }
}
