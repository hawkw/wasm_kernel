//! Implementation of the Mycelium HAL for 64-bit x86 platforms.
#![cfg_attr(not(test), no_std)]
// Allow const operands in asm.
#![feature(asm_const)]
#![feature(abi_x86_interrupt)]
#![feature(doc_cfg)]
// A bunch of const fn features.
#![feature(const_mut_refs)]
// Oftentimes it's necessary to write to a value at a particular location in
// memory, and these types don't implement Copy to ensure they aren't
// inadvertantly copied.
#![allow(clippy::trivially_copy_pass_by_ref)]
// Macros generated by `tracing` often generate large amounts of code, which
// causes this lint to complain about relatively simple methods.
#![allow(clippy::cognitive_complexity)]

pub(crate) use hal_core::{PAddr, VAddr};

pub mod control_regs;
pub mod cpu;
pub mod framebuffer;
pub mod interrupt;
pub mod mm;
pub mod segment;
pub mod serial;
pub mod task;
pub mod time;
pub mod vga;

pub const NAME: &str = "x86_64";
