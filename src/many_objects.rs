use futures::future::{ok, Either, Loop};
use futures::Future;

use client::FrugalosClient;
use slog::Logger;
use std::cmp::min;

pub fn put_many_objects<E>(
    client: FrugalosClient,
    logger: Logger,
    bucket_id: String,
    object_id_prefix: String,
    object_start_index: usize,
    object_count: usize,
    concurrency: usize,
    content: Vec<u8>,
) -> impl Future<Item = (), Error = E> {
    futures::future::loop_fn(
        (0, logger, client, bucket_id, object_id_prefix, content),
        move |(index, logger, client, bucket_id, object_id_prefix, content)| {
            if index >= object_count {
                return Either::A(ok(Loop::Break(())));
            }
            if index % 1000 == 0 {
                info!(
                    logger,
                    "Put done: {} / {} (bucket_id = {}, prefix = {}, index = {})",
                    index,
                    object_count,
                    bucket_id,
                    object_id_prefix,
                    object_start_index + index,
                )
            }
            let mut futures = vec![];
            let this_time = min(object_count - index, concurrency);
            for i in 0..this_time {
                let object_id = format!("{}{}", object_id_prefix, object_start_index + index + i);
                let logger = logger.clone();
                let bucket_id = bucket_id.clone();
                let future = client
                    .request(bucket_id.clone())
                    .put(object_id.clone(), content.clone())
                    .then(move |result| {
                        match track!(result.clone()) {
                            Err(e) => {
                                warn!(
                                    logger,
                                    "Cannot put object (bucket={:?}, object={:?}): {}",
                                    bucket_id,
                                    object_id,
                                    e,
                                );
                            }
                            _ => (),
                        };
                        ok(())
                    });
                futures.push(future);
            }
            let future = futures::future::join_all(futures).map(move |_| {
                Loop::Continue((
                    index + this_time,
                    logger,
                    client,
                    bucket_id,
                    object_id_prefix,
                    content,
                ))
            });
            Either::B(future)
        },
    )
}
