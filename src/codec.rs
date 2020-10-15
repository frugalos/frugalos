use bytecodec::bytes::BytesEncoder;
use bytecodec::io::IoEncodeExt;
use bytecodec::json_codec::JsonEncoder;
use bytecodec::{ByteCount, Encode, Eos, ErrorKind, Result};
use fibers_tasque::{AsyncCall, DefaultCpuTaskQueue, TaskQueueExt};
use futures::{Async, Future};
use std::marker::PhantomData;

use crate::http::HttpResult;
use crate::Error;

#[derive(Debug, Default)]
pub struct AsyncEncoder<E> {
    rx: Option<AsyncCall<Result<Vec<u8>>>>,
    bytes: BytesEncoder<Vec<u8>>,
    _inner: PhantomData<E>,
}
impl<E: Encode + Default> Encode for AsyncEncoder<E>
where
    E::Item: Send + 'static,
{
    type Item = E::Item;

    fn encode(&mut self, buf: &mut [u8], eos: Eos) -> Result<usize> {
        if let Some(mut rx) = self.rx.take() {
            match rx.poll() {
                Err(e) => {
                    track_panic!(ErrorKind::Other, "{}", e);
                }
                Ok(Async::NotReady) => {
                    self.rx = Some(rx);
                    return Ok(0);
                }
                Ok(Async::Ready(result)) => {
                    let bytes = track!(result)?;
                    track!(self.bytes.start_encoding(bytes))?;
                }
            }
        }
        track!(self.bytes.encode(buf, eos))
    }

    fn start_encoding(&mut self, item: Self::Item) -> Result<()> {
        track_assert!(self.is_idle(), ErrorKind::EncoderFull);
        self.rx = Some(DefaultCpuTaskQueue.async_call(move || {
            let mut buf = Vec::new();
            let mut encoder = E::default();
            track!(encoder.start_encoding(item))?;
            track!(encoder.encode_all(&mut buf))?;
            Ok(buf)
        }));
        Ok(())
    }

    fn is_idle(&self) -> bool {
        self.rx.is_none() && self.bytes.is_idle()
    }

    fn requiring_bytes(&self) -> ByteCount {
        if self.rx.is_some() {
            ByteCount::Unknown
        } else {
            self.bytes.requiring_bytes()
        }
    }
}

#[derive(Debug)]
pub enum ObjectResultEncoder {
    Ok(BytesEncoder<Vec<u8>>),
    Err(JsonEncoder<Error>),
}
impl Encode for ObjectResultEncoder {
    type Item = HttpResult<Vec<u8>>;

    fn encode(&mut self, buf: &mut [u8], eos: Eos) -> Result<usize> {
        match *self {
            ObjectResultEncoder::Ok(ref mut e) => track!(e.encode(buf, eos)),
            ObjectResultEncoder::Err(ref mut e) => track!(e.encode(buf, eos)),
        }
    }

    fn start_encoding(&mut self, item: Self::Item) -> Result<()> {
        track_assert!(self.is_idle(), ErrorKind::EncoderFull);
        match item {
            HttpResult::Ok(v) => {
                let mut e = BytesEncoder::default();
                track!(e.start_encoding(v))?;
                *self = ObjectResultEncoder::Ok(e);
            }
            HttpResult::Err(v) => {
                let mut e = JsonEncoder::default();
                track!(e.start_encoding(v))?;
                *self = ObjectResultEncoder::Err(e);
            }
        }
        Ok(())
    }

    fn is_idle(&self) -> bool {
        match *self {
            ObjectResultEncoder::Ok(ref e) => e.is_idle(),
            ObjectResultEncoder::Err(ref e) => e.is_idle(),
        }
    }

    fn requiring_bytes(&self) -> ByteCount {
        match *self {
            ObjectResultEncoder::Ok(ref e) => e.requiring_bytes(),
            ObjectResultEncoder::Err(ref e) => e.requiring_bytes(),
        }
    }
}
impl Default for ObjectResultEncoder {
    fn default() -> Self {
        ObjectResultEncoder::Ok(Default::default())
    }
}
