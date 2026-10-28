//! The byte stream of a replication link: a plain socket, or one wrapped in TLS. The
//! protocol code only reads, writes and sets timeouts, so it takes `&mut dyn Wire`.
use std::{
    io::{self, Read, Write},
    net::TcpStream,
    time::Duration,
};

use rustls::{ClientConnection, ServerConnection, StreamOwned};

pub trait Wire: Read + Write {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_nodelay(&self, nodelay: bool) -> io::Result<()>;
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr>;
}

impl Wire for TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }

    fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        TcpStream::set_nodelay(self, nodelay)
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        TcpStream::set_nonblocking(self, nonblocking)
    }

    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        TcpStream::peer_addr(self)
    }
}

macro_rules! tls_wire {
    ($connection:ty) => {
        impl Wire for StreamOwned<$connection, TcpStream> {
            fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
                self.sock.set_read_timeout(timeout)
            }

            fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
                self.sock.set_write_timeout(timeout)
            }

            fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
                self.sock.set_nodelay(nodelay)
            }

            fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
                self.sock.set_nonblocking(nonblocking)
            }

            fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
                self.sock.peer_addr()
            }
        }
    };
}

tls_wire!(ServerConnection);
tls_wire!(ClientConnection);
