use std::{array, io, result};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Value Error: `{0}`")]
    ValueError(String),

    #[error("The size of the WAL record `{0}` is smaller than the minimum record length `{1}")]
    WalRecordTooSmall(usize, usize),

    #[error("Expected a slice of length `{0}` but received a slice of length `{1}`")]
    InvalidSliceLength(usize, usize),

    #[error("Invalid record type: `{0}`")]
    InvalidRecordType(u8),

    #[error("Expected a CRC value `{0}` but received value `{1}`")]
    InvalidCrc(u32, u32),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("TryFromSliceError error: {0}")]
    TryFromSlice(#[from] array::TryFromSliceError),
}

pub type Result<T> = result::Result<T, Error>;
