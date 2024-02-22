#![warn(missing_docs)]
use super::InvalidDuration;
use crate::cpu::{self, Port};
use core::{
    convert::TryFrom,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use mycelium_util::{
    bits::{bitfield, enum_from_bits},
    fmt,
    sync::spin::Mutex,
};

/// Intel 8253/8254 Programmable Interval Timer (PIT).
///
/// The PIT is a simple timer, with three channels. The most interesting is
/// channel 0, which is capable of firing an interrupt to the [8259 PIC] or [I/O
/// APIC] on ISA interrupt vector 0. Channel 1 was used to time the DRAM refresh
/// rate on ancient IBM PCs and is now generally unused (and may not be
/// implemented in hardware), and channel 2 was connected to the IBM PC speaker
/// and could be used to play sounds.
///
/// The PIT has a non-configurable [base frequency] of 1.193182 MHz, for
/// [extremely cool reasons][reasons], but a 16-bit divisor can be used to
/// determine what multiple of this base frequency each channel fires at.
///
/// [8259 PIC]: crate::interrupt::pic
/// [I/O APIC]: crate::interrupt::apic::IoApic
/// [base frequency]: Self::BASE_FREQUENCY_HZ
/// [reasons]: https://en.wikipedia.org/wiki/Programmable_interval_timer#IBM_PC_compatible
#[derive(Debug)]
pub struct Pit {
    /// PIT channel 0.
    ///
    /// The output from PIT channel 0 is connected to the PIC chip, so that it
    /// generates an IRQ 0. Typically during boot the BIOS sets channel 0 with
    /// a count of 65535 or 0 (which translates to 65536), which gives an output
    /// frequency of 18.2065 Hz (or an IRQ every 54.9254 ms). Channel 0 is
    /// probably the most useful PIT channel, as it is the only channel that is
    /// connected to an IRQ. It can be used to generate an infinte series of
    /// "timer ticks" at a frequency of your choice (as long as it is higher
    /// than 18 Hz), or to generate single CPU interrupts (in "one shot" mode)
    /// after programmable short delays (less than an 18th of a second).
    ///
    /// When choosing an operating mode, below, it is useful to remember that
    /// the IRQ0 is generated by the rising edge of the Channel 0 output voltage
    /// (ie. the transition from "low" to "high", only).
    channel0: Port,
    /// PIT channel 1.
    ///
    /// The output for PIT channel 1 was once used (in conjunction with the DMA
    /// controller's channel 0) for refreshing the DRAM (Dynamic Random Access
    /// Memory) or RAM. Typically, each bit in RAM consists of a capacitor which
    /// holds a tiny charge representing the state of that bit, however (due to
    /// leakage) these capacitors need to be "refreshed" periodically so that
    /// they don't forget their state.
    ///
    /// On later machines, the DRAM refresh is done with dedicated hardware and
    /// the PIT (and DMA controller) is no longer used. On modern computers
    /// where the functionality of the PIT is implemented in a large scale
    /// integrated circuit, PIT channel 1 is no longer usable and may not be
    /// implemented at all.
    ///
    /// In general, this channel should not be used.
    #[allow(dead_code)] // currently, there are no APIs for accessing channel 1
    // TODO(eliza): add APIs for using channel 1 (if it's available)?
    channel1: Port,
    /// PIT channel 2.
    ///
    /// The output of PIT channel 2 is connected to the PC speaker, so the
    /// frequency of the output determines the frequency of the sound produced
    /// by the speaker. This is the only channel where the gate input can be
    /// controlled by software (via bit 0 of I/O port 0x61), and the only
    /// channel where its output (a high or low voltage) can be read by software
    /// (via bit 5 of I/O port 0x61).
    #[allow(dead_code)] // currently, there are no APIs for accessing channel 2
    // TODO(eliza): add APIs for using channel 2 (if it's available)?
    channel2: Port,
    /// PIT command port.
    command: Port,

    /// If PIT channel 0 is configured in periodic mode, this stores the period
    /// as a `Duration` so that we can reset to periodic mode after firing a
    /// sleep interrupt.
    channel0_interval: Option<Duration>,
}

/// Errors returned by [`Pit::start_periodic_timer`] and [`Pit::sleep_blocking`].
#[derive(Debug)]
pub enum PitError {
    /// The periodic timer is already running.
    AlreadyRunning,
    /// A [`Pit::sleep_blocking`] call is in progress.
    SleepInProgress,
    /// The provided duration was invalid.
    InvalidDuration(InvalidDuration),
}

bitfield! {
    struct Command<u8> {
        /// BCD/binary mode.
        ///
        /// The "BCD/Binary" bit determines if the PIT channel will operate in
        /// binary mode or BCD mode (where each 4 bits of the counter represent
        /// a decimal digit, and the counter holds values from 0000 to 9999).
        /// 80x86 PCs only use binary mode (BCD mode is ugly and limits the
        /// range of counts/frequencies possible). Although it should still be
        /// possible to use BCD mode, it may not work properly on some
        /// "compatible" chips. For the "read back" command and the "counter
        /// latch" command, this bit has different meanings.
        const BCD_BINARY: bool;
        /// Operating mode.
        ///
        /// The operating mode bits specify which mode the selected PIT
        /// channel should operate in. For the "read back" command and the
        /// "counter latch" command, these bits have different meanings.
        /// There are 6 different operating modes. See the [`OperatingMode`]
        /// enum for details on the PIT operating modes.
        const MODE: OperatingMode;
        /// Access mode.
        ///
        /// The access mode bits tell the PIT what access mode you wish to use
        /// for the selected channel, and also specify the "counter latch"
        /// command to the CTC. These bits must be valid on every write to the
        /// mode/command register. For the "read back" command, these bits have
        /// a different meaning. For the remaining combinations, these bits
        /// specify what order data will be read and written to the data port
        /// for the associated PIT channel. Because the data port is an 8 bit
        /// I/O port and the values involved are all 16 bit, the PIT chip needs
        /// to know what byte each read or write to the data port wants. For
        /// "low byte only", only the lowest 8 bits of the counter value is read
        /// or written to/from the data port. For "high byte only", only the
        /// highest 8 bits of the counter value is read or written. For the
        /// "low byte/high byte" mode, 16 bits are always transferred as a pair, with
        /// the lowest 8 bits followed by the highest 8 bits (both 8 bit
        /// transfers are to the same IO port, sequentially – a word transfer
        /// will not work).
        const ACCESS: AccessMode;
        /// Channel select.
        ///
        /// The channel select bits select which channel is being configured,
        /// and must always be valid on every write of the mode/command
        /// register, regardless of the other bits or the type of operation
        /// being performed. The ["read back"] (both bits set) is not supported on
        /// the old 8253 chips but should be supported on all AT and later
        /// computers except for PS/2 (i.e. anything that isn't obsolete will
        /// support it).
        ///
        /// ["read back"]: ChannelSelect::ReadBack
        const CHANNEL: ChannelSelect;
    }
}

enum_from_bits! {
    #[derive(Debug, Eq, PartialEq)]
    enum ChannelSelect<u8> {
        Channel0 = 0b00,
        Channel1 = 0b01,
        Channel2 = 0b10,
        /// Readback command (8254 only)
        Readback = 0b11,
    }
}

enum_from_bits! {
    #[derive(Debug, Eq, PartialEq)]
    enum AccessMode<u8> {
        /// Latch count value command
        LatchCount = 0b00,
        /// Access mode: low byte only
        LowByte = 0b01,
        /// Access mode: high byte only
        HighByte = 0b10,
        /// Access mode: both bytes
        Both = 0b11,
    }
}

enum_from_bits! {
    #[derive(Debug, Eq, PartialEq)]
    enum OperatingMode<u8> {
        /// Mode 0 (interrupt on terminal count)
        Interrupt = 0b000,
        /// Mode 1 (hardware re-triggerable one-shot)
        HwOneshot = 0b001,
        /// Mode 2 (rate generator)
        RateGenerator = 0b010,
        /// Mode 3 (square wave generator)
        SquareWave = 0b011,
        /// Mode 4 (software triggered strobe)
        SwStrobe = 0b100,
        /// Mode 5 (hardware triggered strobe)
        HwStrobe = 0b101,
        /// Mode 2 (rate generator, same as `0b010`)
        ///
        /// I'm not sure why both of these exist, but whatever lol.
        RateGenerator2 = 0b110,
        /// Mode 3 (square wave generator, same as `0b011`)
        ///
        /// Again, I don't know why two bit patterns configure the same behavior but
        /// whatever lol.
        SquareWave2 = 0b111,
    }
}

/// The PIT.
///
/// Since a system only has a single PIT, the `Pit` type cannot be constructed
/// publicly and is represented as a singleton. It's stored in a [`Mutex`] in
/// order to ensure that multiple CPU cores don't try to write conflicting
/// configurations to the PIT's configuration ports.
pub static PIT: Mutex<Pit> = Mutex::new(Pit::new());

/// Are we currently sleeping on an interrupt?
static SLEEPING: AtomicBool = AtomicBool::new(false);

impl Pit {
    /// The PIT's base frequency runs at roughly 1.193182 MHz, for [extremely
    /// cool reasons][reasons].
    ///
    /// [reasons]: https://en.wikipedia.org/wiki/Programmable_interval_timer#IBM_PC_compatible
    pub const BASE_FREQUENCY_HZ: usize = 1193180;
    const TICKS_PER_MS: usize = Self::BASE_FREQUENCY_HZ / 1000;

    const fn new() -> Self {
        const BASE: u16 = 0x40;
        Self {
            channel0: Port::at(BASE),
            channel1: Port::at(BASE + 1),
            channel2: Port::at(BASE + 2),
            command: Port::at(BASE + 3),
            channel0_interval: None,
        }
    }

    /// Sleep (by spinning) for `duration`.
    ///
    /// This function sets a flag indicating that a sleep is in progress, and
    /// configures the PIT to fire an interrupt on channel 0 in `duration`. It then
    /// spins until the flag is cleared by an interrupt handler.
    ///
    /// # Usage Notes
    ///
    /// This is a low-level way of sleeping, and is not recommended for use as a
    /// system's primary method of sleeping for a duration. Instead, a timer wheel
    /// or other way of tracking multiple sleepers should be constructed and
    /// advanced based on a periodic timer. This function is provided primarily to
    /// allow using the PIT to calibrate other timers as part of initialization
    /// code, rather than for general purpose use in an operating system.
    ///
    /// In particular, using this function is subject to the following
    /// considerations:
    ///
    /// - An interrupt handler for the PIT interrupt which clears the sleeping flag
    ///   must be installed. This is done automatically by the [`Controller::init`]
    ///   function in the [`interrupt`] module. If that interrupt handler is not
    ///   present, this function will spin forever!
    /// - If the PIT is currently in periodic mode, it will be put in oneshot mode
    ///   when this function is called. This will temporarily disable the existing
    ///   periodic timer.
    /// - This function returns an error if another CPU core is already sleeping. It
    ///   should generally be used only prior to the initialization of application
    ///   processors.
    ///
    /// # Returns
    ///
    /// - [`Ok`]`(())` after `duration` if a sleep was successfully completed.
    /// - [`Err`]`(`[`InvalidDuration`]`)` if the provided duration was
    ///   too long.
    ///
    /// [`Controller::init`]: crate::interrupt::Controller::init
    /// [`interrupt`]: crate::interrupt
    #[tracing::instrument(
        name = "Pit::sleep_blocking"
        level = tracing::Level::DEBUG,
        skip(self),
        fields(?duration),
        err,
    )]
    pub fn sleep_blocking(&mut self, duration: Duration) -> Result<(), PitError> {
        SLEEPING
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| PitError::SleepInProgress)?;
        self.interrupt_in(duration)
            .map_err(PitError::InvalidDuration)?;
        tracing::debug!("started PIT sleep");

        // spin until the sleep interrupt fires.
        while SLEEPING.load(Ordering::Acquire) {
            cpu::wait_for_interrupt();
        }

        tracing::info!(?duration, "slept using PIT channel 0");

        // if we were previously in periodic mode, re-enable it.
        if let Some(interval) = self.channel0_interval {
            tracing::debug!("restarting PIT periodic timer");
            self.start_periodic_timer(interval)?;
        }

        Ok(())
    }

    /// Configures PIT channel 0 in periodic mode, to fire an interrupt every
    /// time the provided `interval` elapses.
    ///
    /// # Returns
    ///
    /// - [`Ok`]`(())` if the timer was successfully configured in periodic
    ///   mode.
    /// - [`Err`]`(`[`InvalidDuration`]`)` if the provided [`Duration`] was
    ///   too long.
    #[tracing::instrument(
        name = "Pit::start_periodic_timer"
        level = tracing::Level::DEBUG,
        skip(self),
        fields(?interval),
        err,
    )]
    pub fn start_periodic_timer(&mut self, interval: Duration) -> Result<(), PitError> {
        if SLEEPING.load(Ordering::Acquire) {
            return Err(PitError::SleepInProgress);
        }

        let interval_ms = usize::try_from(interval.as_millis()).map_err(|_| {
            PitError::invalid_duration(
                interval,
                "PIT periodic timer interval as milliseconds would exceed a `usize`",
            )
        })?;
        let interval_ticks = Self::TICKS_PER_MS * interval_ms;
        let divisor = u16::try_from(interval_ticks).map_err(|_| {
            PitError::invalid_duration(interval, "PIT channel 0 divisor would exceed a `u16`")
        })?;

        // store the periodic timer interval so we can reset later.
        self.channel0_interval = Some(interval);

        // Send the PIT the following command:
        let command = Command::new()
            // use the binary counter
            .with(Command::BCD_BINARY, false)
            // generate a square wave (set the frequency divisor)
            .with(Command::MODE, OperatingMode::SquareWave)
            // we are sending both bytes of the divisor
            .with(Command::ACCESS, AccessMode::Both)
            // and we're configuring channel 0
            .with(Command::CHANNEL, ChannelSelect::Channel0);
        self.send_command(command);
        self.set_divisor(divisor);

        tracing::info!(
            ?interval,
            interval_ms,
            interval_ticks,
            divisor,
            "started PIT periodic timer"
        );

        Ok(())
    }

    /// Configure the PIT to send an IRQ 0 interrupt in `duration`.
    ///
    /// This configures the PIT in mode 0 (oneshot mode). Once the interrupt has
    /// fired, in order to use the periodic timer, the pit must be put back into
    /// periodic mode by calling [`Pit::start_periodic_timer`].
    #[tracing::instrument(
        name = "Pit::interrupt_in"
        level = tracing::Level::DEBUG,
        skip(self),
        fields(?duration),
        err,
    )]
    pub fn interrupt_in(&mut self, duration: Duration) -> Result<(), InvalidDuration> {
        let duration_ms = usize::try_from(duration.as_millis()).map_err(|_| {
            InvalidDuration::new(
                duration,
                "PIT interrupt duration as milliseconds would exceed a `usize`",
            )
        })?;
        let target_time = Self::TICKS_PER_MS * duration_ms;
        let divisor = u16::try_from(target_time).map_err(|_| {
            InvalidDuration::new(
                duration,
                "PIT interrupt target tick count would exceed a `u16`",
            )
        })?;

        tracing::trace!(?duration, duration_ms, target_time, "Pit::interrupt_in");

        let command = Command::new()
            // use the binary counter
            .with(Command::BCD_BINARY, false)
            // generate a square wave (set the frequency divisor)
            .with(Command::MODE, OperatingMode::Interrupt)
            // we are sending both bytes of the divisor
            .with(Command::ACCESS, AccessMode::Both)
            // and we're configuring channel 0
            .with(Command::CHANNEL, ChannelSelect::Channel0);
        self.send_command(command);
        self.set_divisor(divisor);

        Ok(())
    }

    pub(crate) fn handle_interrupt() -> bool {
        SLEEPING.swap(false, Ordering::AcqRel)
    }

    fn set_divisor(&mut self, divisor: u16) {
        tracing::trace!(divisor = &fmt::hex(divisor), "Pit::set_divisor");
        let low = divisor as u8;
        let high = (divisor >> 8) as u8;
        unsafe {
            self.channel0.writeb(low); // write the low byte
            tracing::trace!(lo = &fmt::hex(low), "pit.channel0.writeb(lo)");
            self.channel0.writeb(high); // write the high byte
            tracing::trace!(hi = &fmt::hex(high), "pit.channel0.writeb(hi)");
        }
    }

    fn send_command(&self, command: Command) {
        tracing::debug!(?command, "Pit::send_command");
        unsafe {
            self.command.writeb(command.bits());
        }
    }
}

// === impl PitError ===

impl PitError {
    fn invalid_duration(duration: Duration, msg: &'static str) -> Self {
        Self::InvalidDuration(InvalidDuration::new(duration, msg))
    }
}

impl fmt::Display for PitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRunning => write!(f, "PIT periodic timer is already running"),
            Self::SleepInProgress => write!(f, "a PIT sleep is currently in progress"),
            Self::InvalidDuration(e) => write!(f, "invalid PIT duration: {e}"),
        }
    }
}
