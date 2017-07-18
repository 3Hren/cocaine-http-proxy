use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use futures::{Async, Poll, Stream};

use tokio_core::net::{TcpListener, TcpStream};

/// Byte-oriented stream acceptor.
pub trait Accept {
    /// Stream type.
    type Stream;
    /// Peer address of accepted byte-oriented stream.
    type Addr;

    /// Attempt to accept a connection and create a new connected `Stream` with its peer address if
    /// successful.
    fn accept(&mut self) -> Result<(Self::Stream, Option<Self::Addr>), Error>;
}

/// Represents the stream of sockets received from a listener.
///
/// This will never be exhausted, even on I/O errors unlike the similar struct in the tokio crate.
/// Instead it emits a `Result` on each either successful or not accepting.
pub struct Incoming {
    inner: TcpListener,
}

impl Incoming {
    pub fn new(listener: TcpListener) -> Self {
        Self { inner: listener }
    }
}

impl Stream for Incoming {
    type Item = Result<(TcpStream, SocketAddr), Error>;
    type Error = !;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.inner.accept() {
            Ok((sock, addr)) => Ok(Async::Ready(Some(Ok((sock, addr))))),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => Ok(Async::NotReady),
            Err(err) => Ok(Async::Ready(Some(Err(err)))),
        }
    }
}
