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
        self.0.send(()).map_err(|e| track!(Error::from(e)))
    }
}

impl SignalReceiver {
    /// signalを取り出そうとする。
    /// このメソッドは受信できるsignalがない場合にはエラーを返す。
    pub fn try_recv_signal(&self) -> Result<(), Error> {
        self.0.try_recv().map_err(|e| track!(Error::from(e)))
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

/// `future: Future<Item = (), Error = Error>`からキャンセル可能なFutureを作るための構造体。
/// # Example
///
/// ```ignore
/// fn some_function() -> Result<()> {
///     let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
///     let mut rpc_server_builder = RpcServerBuilder::new(node.addr);
///     ...
///     let rpc_server = rpc_server_builder.finish(executor.handle()).map_err(Error::from);
///     let (cancelable_rpc_server, cancelizer) = Cancelable::new(rpc_server);
///     ...
///     executor.spawn(cancelable_rpc_server.map_err(move |e| panic!("Error: {}", e)));
///     ...
///     // `some_function`の終了直前に、`rpc_server`を終了させるために、`cancelizer.send_signal()`を呼ぶ。
///     track!(cancelizer.send_signal)?;
///     Ok(())
/// }
/// ```
pub struct Cancelable<F> {
    signal_rx: SignalReceiver,
    inner: F,
}

impl<F> Cancelable<F>
where
    F: Future<Item = (), Error = Error> + Send + 'static,
{
    /// 受け取ったfutureをキャンセル可能なfutureに変換する。  
    /// `let (cancelable_future, cancelizer) = Cancelable::new(future)`  
    /// において、`cancelable_future`がキャンセル可能なfutureであり、  
    /// このfutureをキャンセルしたくなった時点で `cancelizer.send_signal()` を呼び出せば、  
    /// シグナル送信以降は `cancelable_future.poll() == Ok(Async::Ready(()))` を即時返却するようになる。
    pub fn new(future: F) -> (Self, SignalSender) {
        let (signal_tx, signal_rx) = make_channel();
        (
            Cancelable {
                inner: future,
                signal_rx,
            },
            signal_tx,
        )
    }
}

impl<F> Future for Cancelable<F>
where
    F: Future<Item = (), Error = Error> + Send + 'static,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(_) = track!(self.signal_rx.poll())? {
            return Ok(Async::Ready(()));
        } else {
            self.inner.poll()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cancelable;
    use fibers::time::timer as fibers_timer;
    use fibers::{Executor, ThreadPoolExecutor};
    use futures::Future;
    use trackable::error::ErrorKindExt;
    use trackable::result::TestResult;
    use {Error, ErrorKind};

    impl From<std::sync::mpsc::RecvError> for Error {
        fn from(f: std::sync::mpsc::RecvError) -> Self {
            ErrorKind::Other.cause(f).into()
        }
    }

    #[test]
    // 停止までに長い時間を要するFutureを作成し、途中でcancelする。
    fn it_stops_loop_future() -> TestResult {
        let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
        let timer = fibers_timer::timeout(std::time::Duration::from_secs(5));
        let timer = timer.map_err(Error::from);
        let (cancelable_future, mut cancelizer) = Cancelable::new(timer);

        // 下の(*)地点でfutureを起動して5秒経過するまでの間に
        // cancel signalを送るためのスレッド。
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            cancelizer.send_signal().unwrap();
        });

        let now = std::time::Instant::now();
        // (*)
        // run_futureはcancelなしの場合に停止までに5秒要するが、
        // 上で別スレッドがそれまでにcancelを送るため、5秒かからずに終了する。
        let result = track!(executor.run_future(cancelable_future).map_err(Error::from))?;
        assert!(result.is_ok());
        assert!(now.elapsed().as_secs() < 3);

        Ok(())
    }

    #[test]
    // 既にキャンセル済みのfutureに対してsignalを送った場合にはエラーになる。
    fn error_occurs_if_send_stop_signal_to_canceled_future() -> TestResult {
        let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
        let timer = fibers_timer::timeout(std::time::Duration::from_secs(5));
        let timer = timer.map_err(Error::from);
        let (cancelable_future, mut cancelizer) = Cancelable::new(timer);
        let mut c = cancelizer.clone();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            c.send_signal().unwrap();
        });

        let now = std::time::Instant::now();
        let run_result = track!(executor.run_future(cancelable_future).map_err(Error::from))?;
        assert!(run_result.is_ok());
        assert!(now.elapsed().as_secs() < 3);

        // 既にcancelされているためエラーとなることを確認する。
        let result = cancelizer.send_signal();
        match result {
            Ok(_) => unreachable!(),
            Err(e) => assert_eq!(*e.kind(), ErrorKind::Other),
        }

        Ok(())
    }
}
