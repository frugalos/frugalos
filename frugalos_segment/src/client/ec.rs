//! Functions and types related to erasure coding.
use ecpool::liberasurecode::LibErasureCoderBuilder;
use ecpool::ErasureCoderPool;
use std::num::NonZeroUsize;

/// ErasureCodingのエンコーダ・デコーダの型。
pub type ErasureCoder = ErasureCoderPool<LibErasureCoderBuilder>;

/// `ErasureCoder`を構築するための補助関数。
pub fn build_ec(data_fragments: usize, parity_fragments: usize) -> ErasureCoder {
    let data_fragments = NonZeroUsize::new(data_fragments).expect("TODO: handle error");
    let parity_fragments = NonZeroUsize::new(parity_fragments).expect("TODO: handle error");
    let builder = LibErasureCoderBuilder::new(data_fragments, parity_fragments);
    ErasureCoderPool::new(builder)
}
