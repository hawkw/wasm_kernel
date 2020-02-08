//! Implementation of the Mycelium HAL for 64-bit x86 platforms.
#![cfg_attr(not(test), no_std)]
#![feature(asm)]
#![feature(abi_x86_interrupt)]
// Oftentimes it's necessary to write to a value at a particular location in
// memory, and these types don't implement Copy to ensure they aren't
// inadvertantly copied.
#![allow(clippy::trivially_copy_pass_by_ref)]
#![allow(clippy::cognitive_complexity)]

pub(crate) use hal_core::VAddr;
pub mod cpu;
pub mod interrupt;
pub mod segment;
pub mod serial;
pub mod tracing;
pub mod vga;

pub const NAME: &str = "x86_64";
