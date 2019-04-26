//! Distributed Tracing 関連の機能を提供する create.

use rustracing::sampler::NullSampler;
use rustracing::tag::StdTag;
use rustracing_jaeger::{Span, Tracer};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use trackable::error::{ErrorKind, TrackableError};

thread_local! {
    static TRACER: RefCell<Option<Tracer>> = RefCell::new(None);
}

/// A tracer containing a thread local `Tracer`.
#[derive(Debug, Clone)]
pub struct ThreadLocalTracer {
    tracer: Arc<Mutex<Tracer>>,
}

impl ThreadLocalTracer {
    /// Returns a new `ThreadLocalTracer`.
    pub fn new(tracer: Tracer) -> Self {
        Self {
            tracer: Arc::new(Mutex::new(tracer)),
        }
    }

    /// Returns a new `Span` applying the specified function.
    pub fn span<F>(&self, f: F) -> Span
    where
        F: FnOnce(&Tracer) -> Span,
    {
        TRACER.with(|local_tracer| {
            if local_tracer.borrow().is_none() {
                if let Ok(global_tracer) = self.tracer.try_lock() {
                    *local_tracer.borrow_mut() = Some(global_tracer.clone());
                }
            }
            if let Some(ref t) = *local_tracer.borrow() {
                f(t)
            } else {
                Span::inactive()
            }
        })
    }
}

/// An extension of `Span`.
pub trait SpanExt {
    /// Logs the specified error into the given span.
    fn log_error<K: ErrorKind>(&mut self, e: &TrackableError<K>);
}

impl SpanExt for Span {
    fn log_error<K: ErrorKind>(&mut self, e: &TrackableError<K>) {
        self.set_tag(StdTag::error);
        self.log(|log| {
            let kind = format!("{:?}", e.kind());
            log.error().kind(kind).message(e.to_string());
        })
    }
}

/// Returns a tracer which samples nothing.
pub fn make_null_tracer() -> ThreadLocalTracer {
    let (tracer, _) = rustracing_jaeger::Tracer::new(NullSampler);
    ThreadLocalTracer::new(tracer)
}
