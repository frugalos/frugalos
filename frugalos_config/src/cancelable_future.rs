use futures::{Async, Future, Poll};
use std::sync::mpsc;
use {Error, ErrorKind};

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

impl SignalReceiver {
    /// signalを取り出そうとする。
    /// このメソッドは受信できるsignalがない場合にはエラーを返す。
    pub fn try_recv_signal(&self) -> Result<(), Error> {
        self.0.try_recv().map_err(Error::from)
    }
}

impl Future for SignalReceiver {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = self.try_recv_signal();

        match result {
            Ok(()) => Ok(Async::Ready(())),
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => Ok(Async::NotReady),
                _ => Err(e),
            },
        }
    }
}

/// Signalの送信口`SignalSender`と受信口`SignalReceiver`を生成する。
pub fn make_channel() -> (SignalSender, SignalReceiver) {
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
        let (signal_tx, signal_rx) = make_channel();
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

#[cfg(test)]
mod tests {
    use super::Cancelable;
    use fibers::{Executor, ThreadPoolExecutor};
    use futures::{Async, Future, Poll};
    use trackable::result::TestResult;
    use {Error, ErrorKind};

    struct S {
        start: std::time::Instant,
        timeout: u64,
    }
    impl S {
        pub fn new(timeout: u64) -> Self {
            S {
                timeout,
                start: std::time::Instant::now(),
            }
        }
    }
    // This future will stop with an Error after `self.timeout`-seconds.
    impl Future for S {
        type Item = ();
        type Error = ();
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            if self.start.elapsed().as_secs() > self.timeout {
                Err(())
            } else {
                Ok(Async::NotReady)
            }
        }
    }

    #[test]
    // 停止までに長い時間を要するFutureを作成し、途中でcancelする。
    fn it_stops_loop_future() -> TestResult {
        let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
        let (cancelable_future, mut cancelizer) = Cancelable::new(S::new(5));

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            cancelizer.send_signal().unwrap();
        });

        let result = executor.run_future(cancelable_future).unwrap();

        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    // 既にキャンセル済みのfutureに対してsignalを送った場合にはエラーになる。
    fn error_occurs_if_send_stop_signal_to_canceled_future() -> TestResult {
        let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
        let (cancelable_future, mut cancelizer) = Cancelable::new(S::new(5));
        let mut c = cancelizer.clone();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            c.send_signal().unwrap();
        });

        executor.run_future(cancelable_future).unwrap().unwrap();

        let result = cancelizer.send_signal();

        match result {
            Ok(_) => unreachable!(),
            Err(e) => assert_eq!(*e.kind(), ErrorKind::Other),
        }

        Ok(())
    }
}
