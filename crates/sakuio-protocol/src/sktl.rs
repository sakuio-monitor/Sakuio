use crate::telemetry::TelemetryBatch;
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// SKTL frame magic bytes.
/// SKTL 帧魔数。
pub const SKTL_MAGIC: [u8; 4] = *b"SKTL";

/// Fixed header length before payload.
/// 载荷前的固定头长度。
pub const SKTL_HEADER_LEN: usize = 16;

/// Bit flag: payload is zstd-compressed CBOR (reserved in v1).
/// 标志位：载荷为 zstd 压缩的 CBOR（v1 保留）。
pub const SKTL_FLAG_ZSTD: u8 = 1 << 0;

/// Maximum telemetry payload size per frame.
/// 单帧遥测载荷上限。
pub const MAX_SKTL_PAYLOAD: usize = 512 * 1024;

/// Decoded telemetry frame before CBOR parse.
/// CBOR 解析前的遥测帧体。
#[derive(Debug, Clone)]
pub struct SkltPayload {
    pub batch_seq: u32,
    pub body: Vec<u8>,
}

/// SKTL encode/decode errors.
/// SKTL 编解码错误。
#[derive(Debug, Error)]
pub enum SktlError {
    #[error("buffer too short")]
    TooShort,
    #[error("invalid magic")]
    BadMagic,
    #[error("unsupported version {0}")]
    BadVersion(u8),
    #[error("payload length {0} exceeds limit")]
    PayloadTooLarge(usize),
    #[error("cbor decode failed")]
    Cbor(#[from] cbor4ii::serde::DecodeError<std::convert::Infallible>),
    #[error("cbor encode failed")]
    CborEncode(#[from] cbor4ii::serde::EncodeError<std::io::Error>),
    #[error("unsupported compression flags {0}")]
    UnsupportedCompression(u8),
    #[error("invalid batch: {0}")]
    InvalidBatch(&'static str),
}

/// Encodes a telemetry batch into a full SKTL binary frame.
/// 将遥测批次编码为完整 SKTL 二进制帧。
pub fn encode_sktl(batch_seq: u32, batch: &TelemetryBatch) -> Result<Vec<u8>, SktlError> {
    let mut body = Vec::new();
    cbor4ii::serde::to_writer(&mut body, batch)?;
    if body.len() > MAX_SKTL_PAYLOAD {
        return Err(SktlError::PayloadTooLarge(body.len()));
    }
    let mut out = BytesMut::with_capacity(SKTL_HEADER_LEN + body.len());
    out.put_slice(&SKTL_MAGIC);
    out.put_u8(crate::SAKULINK_VERSION);
    out.put_u8(0);
    out.put_u16(0);
    out.put_u32(batch_seq);
    out.put_u32(body.len() as u32);
    out.put_slice(&body);
    Ok(out.to_vec())
}

/// Decodes an SKTL frame into batch sequence and telemetry batch.
/// 将 SKTL 帧解码为批次序号与遥测批次。
pub fn decode_sktl(frame: &[u8]) -> Result<(u32, TelemetryBatch), SktlError> {
    if frame.len() < SKTL_HEADER_LEN {
        return Err(SktlError::TooShort);
    }
    let mut cursor = &frame[..];
    let mut magic = [0u8; 4];
    cursor.copy_to_slice(&mut magic);
    if magic != SKTL_MAGIC {
        return Err(SktlError::BadMagic);
    }
    let ver = cursor.get_u8();
    if ver != crate::SAKULINK_VERSION {
        return Err(SktlError::BadVersion(ver));
    }
    let flags = cursor.get_u8();
    let _reserved = cursor.get_u16();
    let batch_seq = cursor.get_u32();
    let len = cursor.get_u32() as usize;
    if len > MAX_SKTL_PAYLOAD {
        return Err(SktlError::PayloadTooLarge(len));
    }
    if cursor.remaining() < len {
        return Err(SktlError::TooShort);
    }
    let payload = &cursor[..len];
    if flags & SKTL_FLAG_ZSTD != 0 {
        return Err(SktlError::UnsupportedCompression(flags));
    }
    let batch: TelemetryBatch = cbor4ii::serde::from_slice(payload)?;
    batch.validate().map_err(SktlError::InvalidBatch)?;
    Ok((batch_seq, batch))
}
