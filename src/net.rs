use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use futures::{Async, Poll, Stream};

use tokio_core::net::{TcpListener, TcpStream};

/// Represents the stream of sockets received from a listener.
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
