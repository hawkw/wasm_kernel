use core::fmt;
use core::mem;
use core::slice;

/// Test descriptor created by `decl_test!`. Describes and allows running an
/// individual test.
pub struct Test {
    pub module: &'static str,
    pub name: &'static str,
    pub run: fn() -> bool,
}

/// Type which may be used as a test return type.
pub trait TestResult {
    /// Report any errors to `tracing`, then returns either `true` for a
    /// success, or `false` for a failure.
    fn report(self) -> bool;
}

impl TestResult for () {
    fn report(self) -> bool {
        true
    }
}

impl<T: fmt::Debug> TestResult for Result<(), T> {
    fn report(self) -> bool {
        match self {
            Ok(_) => true,
            Err(err) => {
                tracing::error!("FAIL {:?}", err);
                false
            }
        }
    }
}

/// Declare a new test, sort-of like the `#[test]` attribute.
// FIXME: Declare a `#[test]` custom attribute instead?
#[macro_export]
macro_rules! decl_test {
    (fn $name:ident $($t:tt)*) => {
        fn $name $($t)*

        // Introduce an anonymous const to avoid name conflicts. The `#[used]`
        // will prevent the symbol from being dropped, and `link_section` will
        // make it visible.
        const _: () = {
            #[used]
            #[link_section = "MyceliumTests"]
            static TEST: $crate::testing::Test = $crate::testing::Test {
                module: module_path!(),
                name: stringify!($name),
                run: || $crate::testing::TestResult::report($name()),
            };
        };
    }
}

// These symbols are auto-generated by lld (and similar linkers) for data
// `link_section` sections, and are located at the beginning and end of the
// section.
//
// The memory region between the two symbols will contain an array of `Test`
// instances.
extern "C" {
    static __start_MyceliumTests: ();
    static __stop_MyceliumTests: ();
}

/// Get a list of `Test` objects.
pub fn all_tests() -> &'static [Test] {
    unsafe {
        // FIXME: These should probably be `&raw const __start_*`.
        let start: *const () = &__start_MyceliumTests;
        let stop: *const () = &__stop_MyceliumTests;

        let len_bytes = (stop as usize) - (start as usize);
        let len = len_bytes / mem::size_of::<Test>();
        assert!(len_bytes % mem::size_of::<Test>() == 0,
                "Section should contain a whole number of `Test`s");

        if len > 0 {
            slice::from_raw_parts(start as *const Test, len)
        } else {
            &[]
        }
    }
}

decl_test! {
    fn it_works() -> Result<(), ()> {
        tracing::info!("I'm running in a test!");
        Ok(())
    }
}
