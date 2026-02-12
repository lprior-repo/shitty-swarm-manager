#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub trait CastSigned {
    type Output;
    fn cast_signed(self) -> Self::Output;
}

pub trait CastUnsigned {
    type Output;
    fn cast_unsigned(self) -> Self::Output;
}

impl CastSigned for u32 {
    type Output = i32;
    fn cast_signed(self) -> Self::Output {
        self as i32
    }
}

impl CastSigned for u64 {
    type Output = i64;
    fn cast_signed(self) -> Self::Output {
        self as i64
    }
}

impl CastSigned for usize {
    type Output = isize;
    fn cast_signed(self) -> Self::Output {
        self as isize
    }
}

impl CastUnsigned for i32 {
    type Output = u32;
    fn cast_unsigned(self) -> Self::Output {
        self as u32
    }
}

impl CastUnsigned for i64 {
    type Output = u64;
    fn cast_unsigned(self) -> Self::Output {
        self as u64
    }
}

impl CastUnsigned for isize {
    type Output = usize;
    fn cast_unsigned(self) -> Self::Output {
        self as usize
    }
}
