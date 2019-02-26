use futures::{Async, Future, Poll};
use std::sync::mpsc;
use Error;

/// Signalを受信するための構造体であり、Futureとしても扱うことができる。  
/// `make_signal`関数によって、対応するSenderとの組として生成される。
pub struct SignalReceiver(mpsc::Receiver<()>);

/// Signalを送信するための構造体である。  
/// `make_signal`関数によって、対応するReceiverとの組として生成される。
#[derive(Clone)]
pub struct SignalSender(mpsc::Sender<()>);

impl SignalSender {
    /// 対応するReceiverにsignalを送信する。
    pub fn send_signal(&mut self) -> Result<(), Error> {
        self.0.send(()).map_err(Error::from)
    }
}

impl Future for SignalReceiver {
    type Item = ();
    type Error = mpsc::TryRecvError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = self.0.try_recv();

        match result {
            Err(mpsc::TryRecvError::Disconnected) => Err(mpsc::TryRecvError::Disconnected),
            Err(mpsc::TryRecvError::Empty) => Ok(Async::NotReady),
            Ok(()) => Ok(Async::Ready(())),
        }
    }
}

/// Signalの送信口`SignalSender`と受信口`SignalReceiver`を生成する。
pub fn make_signal() -> (SignalSender, SignalReceiver) {
    let (sender, receiver) = mpsc::channel();
    (SignalSender(sender), SignalReceiver(receiver))
}

/// `future: Future<Item = (), Error = ()>`からキャンセル可能なFutureを作るための構造体である。
/// # Example
///
/// ```ignore
/// let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
/// let mut rpc_server_builder = RpcServerBuilder::new(node.addr);
/// ...
/// let rpc_server = rpc_server_builder.finish(executor.handle());
/// let (cancelable_rpc_server, cancelizer) =
///     Cancelable::new(rpc_server.map_err(move |e| panic!("Error: {}", e)));
/// ...
/// executor.spawn(cancelable_rpc_server);
/// ...
/// // `some_future`は終了直前に、`rpc_server`を終させるために、`cancelizer.send_signal()`を呼ぶ。
/// executor.run_future(some_future).unwrap();
/// ```
pub struct Cancelable {
    signal_rx: SignalReceiver,
    inner: Box<Future<Item = (), Error = ()> + Send + 'static>,
}

impl Cancelable {
    /// 受け取ったfutureをキャンセル可能なfutureに変換する。  
    /// `let (cancelable_future, cancelizer) = Cancelable::new(future)`  
    /// において、`cancelable_future`がキャンセル可能なfutureであり、  
    /// このfutureをキャンセルしたくなった時点で `cancelizer.send_signal()` を呼び出せば、  
    /// シグナル送信以降は `cancelable_future.poll() == Ok(Async::Ready())` を即時返却するようになる。
    pub fn new<F>(future: F) -> (Self, SignalSender)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        let (signal_tx, signal_rx) = make_signal();
        (
            Cancelable {
                inner: Box::new(future),
                signal_rx,
            },
            signal_tx,
        )
    }
}

impl Future for Cancelable {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(_) = self.signal_rx.poll().unwrap() {
            return Ok(Async::Ready(()));
        } else {
            self.inner.poll()
        }
    }
}
