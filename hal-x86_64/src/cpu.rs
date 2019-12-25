#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Ring {
    Ring0 = 0b00,
    Ring1 = 0b01,
    Ring2 = 0b10,
    Ring3 = 0b11,
}

impl Ring {
    pub fn from_u8(u: u8) -> Self {
        match u {
            0b00 => Ring::Ring0,
            0b01 => Ring::Ring1,
            0b10 => Ring::Ring2,
            0b11 => Ring::Ring3,
            bits => panic!("invalid ring {:#02b}", bits),
        }
    }
}
