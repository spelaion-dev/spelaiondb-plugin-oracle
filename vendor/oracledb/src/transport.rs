//-----------------------------------------------------------------------------
// Copyright (c) 2026, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// transport.rs
//
// Defines the structures and methods used for managing the transport used for
// sending and receiving messages.
//-----------------------------------------------------------------------------

use pkcs8::DecodePrivateKey;
use rustls::ClientConfig as TlsClientConfig;
use rustls::ClientConnection as TlsClientConnection;
use rustls::StreamOwned as TlsStream;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;
use rustls::sign::CertifiedKey;

use std::cmp::min;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::config::Address;
use crate::config::Config;
use crate::constants;
use crate::error::Error;
use crate::packet::Packet;

enum LowLevelTransport {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TlsClientConnection, TcpStream>>),
}

impl LowLevelTransport {
    /// Flushes all data to the low level transport.
    fn flush(&mut self) -> Result<(), Error> {
        match self {
            Self::Tcp(s) => Ok(s.flush()?),
            Self::Tls(s) => Ok(s.flush()?),
        }
    }

    /// Returns the read timeout associated with the low level transport.
    pub(crate) fn get_read_timeout(
        &self,
    ) -> Result<Option<std::time::Duration>, Error> {
        match self {
            Self::Tcp(s) => Ok(s.read_timeout()?),
            Self::Tls(s) => Ok(s.sock.read_timeout()?),
        }
    }

    /// Neogtiate TLS
    fn negotiate_tls(
        self,
        server_name: &str,
        config: &Config,
    ) -> Result<Self, Error> {
        let mut resolver = CustomClientCertResolver::new();
        if let Some(pem) = config.tls_roots_pem() {
            resolver.use_roots(pem)?;
        }
        if let Some(wallet_location) = config.wallet_location() {
            resolver.populate(
                wallet_location,
                config.get_wallet_password_bytes(),
            )?;
        }
        let tls_config = resolver.get_tls_config()?;
        // Patched for SpelaionDB (PATCHED.md, 2): a name rustls cannot use is an
        // error, not a panic.
        let tls_server_name: rustls::pki_types::ServerName =
            server_name.to_string().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{server_name:?} is not a host name TLS can check"),
                )
            })?;
        let conn = rustls::ClientConnection::new(
            Arc::new(tls_config),
            tls_server_name,
        )?;
        let stream = match self {
            Self::Tcp(stream) => stream,
            Self::Tls(orig_stream) => {
                let (_, stream) = orig_stream.into_parts();
                stream
            }
        };
        let tls_stream = rustls::StreamOwned::new(conn, stream);
        Ok(Self::Tls(Box::new(tls_stream)))
    }

    /// Reads bytes from the low level transport and returns the number of
    /// bytes read.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        match self {
            Self::Tcp(s) => Ok(s.read(buf)?),
            Self::Tls(s) => Ok(s.read(buf)?),
        }
    }

    /// Sets the read and write timeouts for the low level transport.
    pub(crate) fn set_timeout(
        &self,
        duration: Option<std::time::Duration>,
    ) -> Result<(), Error> {
        let stream = match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => &s.sock,
        };
        stream.set_read_timeout(duration)?;
        Ok(stream.set_write_timeout(duration)?)
    }

    /// Patched for SpelaionDB (PATCHED.md, 1): the read timeout alone.
    fn set_read_timeout(&self, duration: Option<Duration>) -> Result<(), Error> {
        let stream = match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => &s.sock,
        };
        Ok(stream.set_read_timeout(duration)?)
    }

    /// Patched for SpelaionDB (PATCHED.md, 1): the write timeout alone.
    fn set_write_timeout(
        &self,
        duration: Option<Duration>,
    ) -> Result<(), Error> {
        let stream = match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => &s.sock,
        };
        Ok(stream.set_write_timeout(duration)?)
    }

    /// Shuts down the low-level transport.
    fn shutdown(&mut self) -> Result<(), Error> {
        let stream = match self {
            Self::Tcp(s) => s,
            Self::Tls(s) => {
                s.conn.send_close_notify();
                s.flush()?;
                &mut s.sock
            }
        };
        let _ = stream.shutdown(std::net::Shutdown::Both);
        Ok(())
    }

    /// Writes bytes to the low level transport.
    fn write_all(&mut self, data: &[u8]) -> Result<(), Error> {
        match self {
            Self::Tcp(s) => Ok(s.write_all(data)?),
            Self::Tls(s) => Ok(s.write_all(data)?),
        }
    }
}

pub(crate) struct Transport {
    low_level_transport: Option<LowLevelTransport>,
    max_packet_size: usize,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    residual_bytes: usize,
    last_packet_bytes: usize,
    full_packet_size: bool,
    // Patched for SpelaionDB (PATCHED.md, 1): set by a `CancelHandle`; checked
    // every `CANCEL_TICK` while waiting for the database.
    cancel: Option<Arc<AtomicBool>>,
    // The call timeout, kept here once reads wake up every `CANCEL_TICK`.
    call_timeout: Option<Duration>,
}

// Patched for SpelaionDB (PATCHED.md, 1): how often a read waiting for the
// database wakes up to see whether its call was cancelled.
const CANCEL_TICK: Duration = Duration::from_millis(100);

// a custom client certificate resolver is required because the client
// certificates supplied by Oracle Database are v1 certificates and rustls will
// not accept anything less than v3 certificates
#[derive(Debug)]
struct CustomClientCertResolver {
    provider: Arc<CryptoProvider>,
    key: Option<Arc<CertifiedKey>>,
    root_store: Option<rustls::RootCertStore>,
}

// PEM file header/footers
const PEM_ENCRYPTED_PRIVATE_KEY_HEADER: &str =
    "-----BEGIN ENCRYPTED PRIVATE KEY-----";
const PEM_ENCRYPTED_PRIVATE_KEY_FOOTER: &str =
    "-----END ENCRYPTED PRIVATE KEY-----";
const PEM_UNENCRYPTED_PRIVATE_KEY_HEADER: &str = "-----BEGIN PRIVATE KEY-----";

impl Transport {
    /// Extracts a packet from the data that has been received from the
    /// database. If insufficient bytes have been read, None is returned which
    /// signals that the caller should wait for some more bytes to be received.
    fn extract_packet(&mut self) -> Option<Packet> {
        if self.residual_bytes <= 4 {
            return None;
        }
        self.last_packet_bytes = self.get_packet_size();
        if self.residual_bytes < self.last_packet_bytes {
            return None;
        }
        self.residual_bytes -= self.last_packet_bytes;
        let packet_buf = &self.read_buf[0..self.last_packet_bytes];
        Some(Packet::new(packet_buf))
    }

    /// Returns the size of the packet by examining the packet header. The
    /// first few packets use a 16-bit packet size but once negotiation is
    /// complete a 32-bit packet size is used.
    fn get_packet_size(&self) -> usize {
        if self.full_packet_size {
            let buf = &self.read_buf[0..4];
            u32::from_be_bytes(buf.try_into().unwrap()) as usize
        } else {
            let buf = &self.read_buf[0..2];
            u16::from_be_bytes(buf.try_into().unwrap()) as usize
        }
    }

    /// Attempts to read a packet from the stream, if it is connected. The
    /// number of bytes read is returned.
    fn read_packet(&mut self) -> Result<usize, Error> {
        let buf = &mut self.read_buf[self.residual_bytes..];
        self.low_level_transport
            .as_mut()
            .ok_or_else(Error::not_connected)
            .and_then(|t| t.read(buf))
    }

    /// Writes a packet to the stream, if the stream is currently connected.
    fn write_packet(&mut self) -> Result<(), Error> {
        if self.low_level_transport.is_none() {
            return Err(Error::not_connected());
        }
        let data = &self.write_buf[..];
        if let Some(t) = self.low_level_transport.as_mut() {
            t.write_all(data)?;
        }
        Ok(())
    }

    /// Closes the TCP connection to the database.
    pub(crate) fn close(&mut self) -> Result<(), Error> {
        self.low_level_transport
            .take()
            .ok_or_else(Error::not_connected)
            .and_then(|mut t| t.shutdown())
    }

    /// Establishes a TCP connection to the database.
    pub(crate) fn connect(
        &mut self,
        stream: TcpStream,
        address: &Address,
        config: &Config,
    ) -> Result<(), Error> {
        stream.set_nodelay(true)?;
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;
        self.low_level_transport = Some(LowLevelTransport::Tcp(stream));
        if address.protocol() == "tcps" {
            // Patched for SpelaionDB (PATCHED.md, 2): the certificate is checked
            // against the configured server name, not the address connected to
            // (a tunnel's local end).
            let server_name =
                config.tls_server_name().unwrap_or(address.host()).to_string();
            self.negotiate_tls(&server_name, config)?;
        }
        Ok(())
    }

    /// Returns the read timeout associated with the transport.
    pub(crate) fn get_read_timeout(
        &self,
    ) -> Result<Option<std::time::Duration>, Error> {
        self.low_level_transport
            .as_ref()
            .ok_or_else(Error::not_connected)
            .and_then(|t| t.get_read_timeout())
    }

    /// Negotiates TLS on the connection.
    pub(crate) fn negotiate_tls(
        &mut self,
        server_name: &str,
        config: &Config,
    ) -> Result<(), Error> {
        let low_level_transport = self.low_level_transport.take().unwrap();
        self.low_level_transport =
            Some(low_level_transport.negotiate_tls(server_name, config)?);
        Ok(())
    }

    /// Creates a new empty transport.
    pub(crate) fn new() -> Self {
        Self {
            low_level_transport: None,
            max_packet_size: 0,
            read_buf: Vec::new(),
            write_buf: Vec::new(),
            residual_bytes: 0,
            last_packet_bytes: 0,
            full_packet_size: false,
            cancel: None,
            call_timeout: None,
        }
    }

    /// Reads data from the database and returns a single packet, or an error
    /// indicating that the connection is no longer valid.
    pub(crate) fn receive_packet(&mut self) -> Result<Packet, Error> {
        if self.residual_bytes > 0 && self.last_packet_bytes > 0 {
            let start_pos = self.last_packet_bytes;
            let end_pos = self.last_packet_bytes + self.residual_bytes;
            self.read_buf.copy_within(start_pos..end_pos, 0);
            self.last_packet_bytes = 0;
        }
        let started = Instant::now();
        loop {
            if let Some(packet) = self.extract_packet() {
                return Ok(packet);
            }
            let num_bytes = match self.read_packet() {
                Ok(num_bytes) => num_bytes,
                // Patched for SpelaionDB (PATCHED.md, 1): with cancels enabled a
                // read wakes up every `CANCEL_TICK`. The bytes read so far stay
                // in `read_buf`, so reading goes on where it stopped unless the
                // call was cancelled or its timeout has passed.
                Err(err) if err.is_call_timeout_exceeded() => {
                    let Some(flag) = &self.cancel else {
                        return Err(err);
                    };
                    if flag.swap(false, Ordering::SeqCst) {
                        return Err(Error::cancelled());
                    }
                    if self
                        .call_timeout
                        .is_some_and(|limit| started.elapsed() >= limit)
                    {
                        return Err(err);
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };
            if num_bytes == 0 {
                self.low_level_transport = None;
                break;
            }
            self.residual_bytes += num_bytes;
        }
        Err(Error::dead_connection())
    }

    pub(crate) fn send_packets(
        &mut self,
        packet_type: u8,
        packet_flags: u8,
        data_flags: u16,
        mut data: &[u8],
    ) -> Result<(), Error> {
        let mut header_size = 8;
        if packet_type == constants::PACKET_TYPE_DATA {
            header_size += 2;
        }
        let max_data_size = self.max_packet_size - header_size;
        loop {
            let packet_data_size = min(max_data_size, data.len());
            let packet_data = &data[..packet_data_size];
            data = &data[packet_data_size..];
            let packet_size = packet_data.len() + header_size;
            self.write_buf.clear();
            if self.full_packet_size {
                let value: u32 = packet_size.try_into().unwrap();
                self.write_buf.extend(value.to_be_bytes());
            } else {
                let value: u16 = packet_size.try_into().unwrap();
                self.write_buf.extend(value.to_be_bytes());
                self.write_buf.push(0);
                self.write_buf.push(0);
            }
            self.write_buf.push(packet_type);
            self.write_buf.push(packet_flags);
            self.write_buf.push(0);
            self.write_buf.push(0);
            if packet_type == constants::PACKET_TYPE_DATA {
                self.write_buf.extend(data_flags.to_be_bytes());
            }
            self.write_buf.extend(packet_data);
            self.write_packet()?;
            if data.is_empty() {
                break;
            }
        }
        self.low_level_transport
            .as_mut()
            .ok_or_else(Error::not_connected)
            .and_then(|t| t.flush())
    }

    /// Sets the maximum packet size for the transport since it may vary for
    /// each description that is specified in the connect string.
    pub(crate) fn set_max_packet_size(&mut self, size: usize) {
        self.read_buf.resize(size, 0);
        self.write_buf.clear();
        self.write_buf.reserve_exact(size);
        self.max_packet_size = size;
    }

    /// Patched for SpelaionDB (PATCHED.md, 1): from now on a read waiting for
    /// the database wakes up every `CANCEL_TICK` and stops the call when `flag`
    /// is set. Writes keep the call timeout.
    pub(crate) fn enable_cancel(
        &mut self,
        flag: Arc<AtomicBool>,
    ) -> Result<(), Error> {
        let transport = self
            .low_level_transport
            .as_ref()
            .ok_or_else(Error::not_connected)?;
        self.call_timeout = transport.get_read_timeout()?;
        transport.set_read_timeout(Some(CANCEL_TICK))?;
        self.cancel = Some(flag);
        Ok(())
    }

    /// Patched for SpelaionDB (PATCHED.md, 1): forgets a cancel asked for
    /// before the call that is starting.
    pub(crate) fn clear_cancel(&self) {
        if let Some(flag) = &self.cancel {
            flag.store(false, Ordering::SeqCst);
        }
    }

    /// Returns the call timeout: the socket's read timeout, or the one kept
    /// apart once cancels are enabled (PATCHED.md, 1).
    pub(crate) fn get_call_timeout(&self) -> Result<Option<Duration>, Error> {
        if self.cancel.is_some() {
            return Ok(self.call_timeout);
        }
        self.get_read_timeout()
    }

    /// Sets the read and write timeouts for the transport.
    pub(crate) fn set_timeout(
        &mut self,
        duration: Option<std::time::Duration>,
    ) -> Result<(), Error> {
        if self.cancel.is_some() {
            self.call_timeout = duration;
            return self
                .low_level_transport
                .as_ref()
                .ok_or_else(Error::not_connected)
                .and_then(|t| t.set_write_timeout(duration));
        }
        self.low_level_transport
            .as_ref()
            .ok_or_else(Error::not_connected)
            .and_then(|t| t.set_timeout(duration))
    }

    /// Indicates that the full packet size should be used for all subsequent
    /// packets sent on the transport.
    pub(crate) fn set_full_packet_size(&mut self) {
        self.full_packet_size = true;
    }

    /// Returns whether this transport is currently wrapped in TLS.
    pub(crate) fn uses_tls(&self) -> bool {
        matches!(self.low_level_transport, Some(LowLevelTransport::Tls(_)))
    }
}

impl CustomClientCertResolver {
    /// Creates a new empty structure and returns it.
    fn new() -> Self {
        Self {
            // Patched for SpelaionDB (PATCHED.md, 4): ring, never aws-lc-rs.
            provider: CryptoProvider::get_default().cloned().unwrap_or_else(
                || Arc::new(rustls::crypto::ring::default_provider()),
            ),
            key: None,
            root_store: Some(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            }),
        }
    }

    /// Returns the TLS configuration to use when connecting to the database.
    fn get_tls_config(mut self) -> Result<TlsClientConfig, Error> {
        let root_store = self.root_store.take().unwrap();
        Ok(TlsClientConfig::builder_with_provider(self.provider.clone())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_store)
            .with_client_cert_resolver(Arc::new(self)))
    }

    /// Populates the certified key with the contents of the wallet defined in
    /// the configuration, if one was defined. If the wallet does not contain a
    /// private key, then the certificates that are contained in the wallet are
    /// returned directly and used as part of the certificate roots used to
    /// validate the server certificate instead.
    fn populate(
        &mut self,
        wallet_location: &str,
        wallet_password: Vec<u8>,
    ) -> Result<(), Error> {
        let file_name = Path::new(wallet_location).join("ewallet.pem");
        let contents = fs::read_to_string(&file_name).map_err(|e| {
            Error::wallet_missing(e, file_name.display().to_string())
        })?;

        // retrieve any certificates
        let mut certs: Vec<CertificateDer> = Vec::new();
        for result in CertificateDer::pem_slice_iter(contents.as_bytes()) {
            certs.push(result?);
        }

        // if an encrypted private key is found, the pkcs8 crate is
        // required to decrypt it using the wallet password
        let mut private_key: Option<PrivateKeyDer> = None;
        if let Some(start_pos) =
            contents.find(PEM_ENCRYPTED_PRIVATE_KEY_HEADER)
            && let Some(end_pos) =
                contents.find(PEM_ENCRYPTED_PRIVATE_KEY_FOOTER)
        {
            let part = &contents
                [start_pos..end_pos + PEM_ENCRYPTED_PRIVATE_KEY_FOOTER.len()];
            let (_label, doc) = pkcs8::SecretDocument::from_pem(part)
                .map_err(|e| {
                    Error::wallet_private_key_invalid(e.to_string())
                })?;
            let decrypted_key =
                pkcs8::SecretDocument::from_pkcs8_encrypted_der(
                    doc.as_bytes(),
                    wallet_password,
                )
                .map_err(|e| {
                    Error::wallet_password_missing_or_invalid(
                        file_name.display().to_string(),
                        e.to_string(),
                    )
                })?;
            private_key = Some(PrivateKeyDer::Pkcs8(
                decrypted_key.as_bytes().to_vec().into(),
            ));

        // for the unencrypted private key, the rustls crate can be used
        } else if contents.contains(PEM_UNENCRYPTED_PRIVATE_KEY_HEADER) {
            private_key = Some(
                PrivateKeyDer::from_pem_slice(contents.as_bytes()).map_err(
                    |e| Error::wallet_private_key_invalid(e.to_string()),
                )?,
            );
        }

        // if a private key was found, setup the certified key to pass to the
        // server
        if let Some(key) = private_key {
            let signing_key =
                self.provider.key_provider.load_private_key(key)?;
            self.key = Some(Arc::new(CertifiedKey::new(certs, signing_key)));
        } else {
            let root_store: &mut rustls::RootCertStore =
                self.root_store.as_mut().unwrap();
            for cert in &certs {
                root_store.add(cert.clone())?;
            }
        }

        Ok(())
    }

    /// Patched for SpelaionDB (PATCHED.md, 2): trusts only the PEM certificates
    /// in `pem`, instead of the built-in roots.
    fn use_roots(&mut self, pem: &[u8]) -> Result<(), Error> {
        let mut root_store = rustls::RootCertStore::empty();
        for result in CertificateDer::pem_slice_iter(pem) {
            root_store.add(result?)?;
        }
        self.root_store = Some(root_store);
        Ok(())
    }
}

impl rustls::client::ResolvesClientCert for CustomClientCertResolver {
    fn resolve(
        &self,
        _acceptable_issuers: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        self.key.clone()
    }

    fn has_certs(&self) -> bool {
        self.key.is_some()
    }
}
