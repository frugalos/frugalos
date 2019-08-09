use futures::{Future, Poll};

use Error;

#[derive(Debug)]
pub enum Phase<A, B> {
    A(A),
    B(B),
}
impl<A, B> Future for Phase<A, B>
where
    A: Future<Error = Error>,
    B: Future<Error = Error>,
{
    type Item = Phase<A::Item, B::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            Phase::A(f) => track!(f.poll()).map(|t| t.map(Phase::A)),
            Phase::B(f) => track!(f.poll()).map(|t| t.map(Phase::B)),
        }
    }
}

#[derive(Debug)]
pub enum Phase3<A, B, C> {
    A(A),
    B(B),
    C(C),
}
impl<A, B, C> Future for Phase3<A, B, C>
where
    A: Future<Error = Error>,
    B: Future<Error = Error>,
    C: Future<Error = Error>,
{
    type Item = Phase3<A::Item, B::Item, C::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            Phase3::A(f) => track!(f.poll()).map(|t| t.map(Phase3::A)),
            Phase3::B(f) => track!(f.poll()).map(|t| t.map(Phase3::B)),
            Phase3::C(f) => track!(f.poll()).map(|t| t.map(Phase3::C)),
        }
    }
}

pub(crate) type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send + 'static>;

pub(crate) fn into_box_future<F>(future: F) -> BoxFuture<F::Item>
where
    F: Future<Error = cannyls::Error> + Send + 'static,
{
    Box::new(future.map_err(Error::from))
}
