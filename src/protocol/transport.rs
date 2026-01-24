use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use crate::error::{Error, Result};
use crate::protocol::packet::{PacketHeader, PacketBuilder, MessageType, MAX_PACKET_SIZE};

/// Simple UDP transport layer
///
/// Handles raw UDP send/receive with basic packet framing.
/// Message sequencing and ACKs are handled at a higher level.
pub struct Transport {
    socket: UdpSocket,
    remote_addr: SocketAddr,
    next_msg_id: u16,
}

impl Transport {
    pub async fn new(remote_addr: SocketAddr) -> Result<Self> {
        let bind_addr = match remote_addr {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketAddr::V6(_) => SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0)),
        };
        Self::new_with_bind(remote_addr, bind_addr).await
    }

    pub async fn new_with_bind(remote_addr: SocketAddr, bind_addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await
            .map_err(|e| Error::Io(e.to_string()))?;

        // Set receive buffer to 4MB to handle burst TransferBlock responses
        #[cfg(unix)]
        unsafe {
            let buf_size: libc::c_int = 4 * 1024 * 1024;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        Ok(Self {
            socket,
            remote_addr,
            next_msg_id: 1,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr()
            .map_err(|e| Error::Io(e.to_string()))
    }

    /// Get the next message ID and increment
    pub fn next_message_id(&mut self) -> u16 {
        let id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);
        if self.next_msg_id == 0 {
            self.next_msg_id = 1; // Skip 0
        }
        id
    }

    /// Send a raw packet
    pub async fn send_raw(&mut self, data: &[u8]) -> Result<()> {
        self.socket.send_to(data, self.remote_addr).await
            .map_err(|e| Error::Io(e.to_string()))?;
        Ok(())
    }

    /// Send a message with automatic message ID
    pub async fn send(&mut self, msg_type: MessageType, payload: &[u8], reliable: bool) -> Result<u16> {
        let msg_id = self.next_message_id();
        let packet = PacketBuilder::new(msg_type, msg_id, reliable)
            .payload(payload)
            .build();
        self.send_raw(&packet).await?;
        Ok(msg_id)
    }

    /// Receive a packet, returns (message_type, message_id, payload)
    pub async fn recv(&mut self) -> Result<(MessageType, u16, Vec<u8>)> {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];

        let (len, from) = self.socket.recv_from(&mut buf).await
            .map_err(|e| Error::Io(e.to_string()))?;

        if from != self.remote_addr {
            return Err(Error::InvalidPacket("packet from wrong address".into()));
        }

        buf.truncate(len);
        let (header, payload_start) = PacketHeader::parse(&buf)?;
        let payload = buf[payload_start..].to_vec();

        Ok((header.message_type, header.message_id, payload))
    }

    /// Receive with timeout
    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<(MessageType, u16, Vec<u8>)>> {
        match tokio::time::timeout(timeout, self.recv()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Try to receive raw bytes without blocking. Returns None if no data available.
    pub fn try_recv_raw(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];
        match self.socket.try_recv_from(&mut buf) {
            Ok((len, _)) => {
                buf.truncate(len);
                Ok(Some(buf))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(Error::Io(e.to_string())),
        }
    }

    /// Receive raw bytes with timeout
    pub async fn recv_raw_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];
        match tokio::time::timeout(timeout, self.socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => {
                buf.truncate(len);
                Ok(Some(buf))
            }
            Ok(Err(e)) => Err(Error::Io(e.to_string())),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_creation() {
        let addr: SocketAddr = "127.0.0.1:34197".parse().unwrap();
        let transport = Transport::new(addr).await.unwrap();
        assert!(transport.local_addr().is_ok());
    }
}
