use futures::{Future, Poll};
use raftlog::Error;

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
pub enum Phase5<A, B, C, D, E> {
    A(A),
    B(B),
    C(C),
    D(D),
    E(E),
}
impl<A, B, C, D, E> Future for Phase5<A, B, C, D, E>
where
    A: Future<Error = Error>,
    B: Future<Error = Error>,
    C: Future<Error = Error>,
    D: Future<Error = Error>,
    E: Future<Error = Error>,
{
    #[allow(clippy::type_complexity)]
    type Item = Phase5<A::Item, B::Item, C::Item, D::Item, E::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            Phase5::A(f) => track!(f.poll()).map(|t| t.map(Phase5::A)),
            Phase5::B(f) => track!(f.poll()).map(|t| t.map(Phase5::B)),
            Phase5::C(f) => track!(f.poll()).map(|t| t.map(Phase5::C)),
            Phase5::D(f) => track!(f.poll()).map(|t| t.map(Phase5::D)),
            Phase5::E(f) => track!(f.poll()).map(|t| t.map(Phase5::E)),
        }
    }
}
