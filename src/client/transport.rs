use super::*;

/// Blocking thread that reads server messages and tags them with their connection generation.
pub(super) fn server_reader_thread(
    mut stream: LocalStream,
    event_tx: tokio::sync::mpsc::Sender<ClientLoopEvent>,
    should_quit: &Arc<AtomicBool>,
    max_frame_size: usize,
    endpoint_id: endpoint::ClientEndpointId,
    generation: u64,
) {
    if stream.set_nonblocking(false).is_err() {
        let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
            endpoint_id,
            generation,
        });
        return;
    }

    loop {
        if should_quit.load(Ordering::Acquire) {
            break;
        }

        match protocol::read_message(&mut stream, max_frame_size) {
            Ok(msg) => {
                if event_tx
                    .blocking_send(ClientLoopEvent::ServerMessage {
                        endpoint_id: endpoint_id.clone(),
                        generation,
                        message: Box::new(msg),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(protocol::FramingError::UnexpectedEof) => {
                let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
                    endpoint_id: endpoint_id.clone(),
                    generation,
                });
                break;
            }
            Err(protocol::FramingError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => {
                warn!(err = %err, "server read error");
                let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
                    endpoint_id: endpoint_id.clone(),
                    generation,
                });
                break;
            }
        }
    }
}

pub(in crate::client) fn write_to_local_server(
    stream: &mut LocalStream,
    msg: &ClientMessage,
) -> io::Result<()> {
    protocol::write_message(stream, msg).map_err(|error| io::Error::other(error.to_string()))
}

pub(super) trait ClientMessageSink {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()>;
}

impl ClientMessageSink for LocalStream {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()> {
        write_to_local_server(self, message)
    }
}

impl ClientMessageSink for endpoint::EndpointRegistry {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()> {
        let active = self.active_id().clone();
        if self.send(message) == endpoint::EndpointSendOutcome::Sent || !active.is_local() {
            return Ok(());
        }
        let failure = self
            .take_failures()
            .into_iter()
            .find(|failure| failure.endpoint_id == active);
        Err(failure.map_or_else(
            || io::Error::new(io::ErrorKind::BrokenPipe, "local endpoint is unavailable"),
            |failure| io::Error::new(failure.kind, failure.message),
        ))
    }
}

pub(super) fn write_to_server(
    stream: &mut impl ClientMessageSink,
    msg: &ClientMessage,
) -> io::Result<()> {
    stream.send_client_message(msg)
}
