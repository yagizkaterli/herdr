use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use interprocess::TryClone as _;

use super::{ClientEndpointId, ClientEndpointStatus, EndpointNegotiation, NativeEndpointTransport};
use crate::protocol::{ClientSurfaceSize, RenderEncoding};

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(crate) struct SshConnectOptions {
    pub(crate) cols: u16,
    pub(crate) rows: u16,
    pub(crate) cell_width_px: u32,
    pub(crate) cell_height_px: u32,
    pub(crate) pixel_geometry_exact: bool,
    pub(crate) surface_size: ClientSurfaceSize,
    pub(crate) endpoint_keybindings: bool,
    pub(crate) mouse_capture: bool,
}

pub(crate) enum SshSupervisorEvent {
    Status {
        endpoint_id: ClientEndpointId,
        generation: u64,
        status: ClientEndpointStatus,
        message: String,
    },
    Connected {
        endpoint_id: ClientEndpointId,
        generation: u64,
        reader: crate::ipc::LocalStream,
        writer: NativeEndpointTransport,
        negotiation: EndpointNegotiation,
    },
}

struct ProfileReconnectState {
    profile: super::SavedSshEndpoint,
    attempts: u32,
    next_attempt: Option<Instant>,
    in_flight: bool,
    generation: Option<u64>,
}

pub(crate) struct SshSupervisors {
    profiles: HashMap<ClientEndpointId, ProfileReconnectState>,
    next_generation: u64,
    shutdown: Arc<AtomicBool>,
}

impl SshSupervisors {
    pub(crate) fn new(profiles: &[super::SavedSshEndpoint], now: Instant) -> Self {
        let profiles = profiles
            .iter()
            .filter(|profile| profile.enabled)
            .map(|profile| {
                (
                    ClientEndpointId::Ssh(profile.id.clone()),
                    ProfileReconnectState {
                        profile: profile.clone(),
                        attempts: 0,
                        next_attempt: Some(now),
                        in_flight: false,
                        generation: None,
                    },
                )
            })
            .collect();
        Self {
            profiles,
            next_generation: 2,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn spawn_due(
        &mut self,
        now: Instant,
        options: SshConnectOptions,
        event_tx: &tokio::sync::mpsc::Sender<SshSupervisorEvent>,
    ) {
        for (endpoint_id, state) in &mut self.profiles {
            if state.in_flight || state.next_attempt.is_none_or(|deadline| deadline > now) {
                continue;
            }
            state.in_flight = true;
            state.next_attempt = None;
            let generation = self.next_generation;
            state.generation = Some(generation);
            self.next_generation = self.next_generation.saturating_add(1);
            let endpoint_id = endpoint_id.clone();
            let profile = state.profile.clone();
            let event_tx = event_tx.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                let task_endpoint_id = endpoint_id.clone();
                let result = tokio::task::spawn_blocking(move || {
                    connect_once(&profile, options, endpoint_id, generation)
                })
                .await;
                let event = match result {
                    Ok(Ok(event)) => event,
                    Ok(Err((endpoint_id, error))) => SshSupervisorEvent::Status {
                        endpoint_id,
                        generation,
                        status: if crate::remote::saved_ssh_failure_needs_attention(&error) {
                            ClientEndpointStatus::Attention
                        } else {
                            ClientEndpointStatus::Reconnecting
                        },
                        message: error.to_string(),
                    },
                    Err(error) => SshSupervisorEvent::Status {
                        endpoint_id: task_endpoint_id,
                        generation,
                        status: ClientEndpointStatus::Reconnecting,
                        message: format!("SSH connection task stopped unexpectedly: {error}"),
                    },
                };
                if !shutdown.load(Ordering::Acquire) {
                    let _ = event_tx.send(event).await;
                }
            });
        }
    }

    pub(crate) fn record_status(
        &mut self,
        endpoint_id: &ClientEndpointId,
        generation: u64,
        status: ClientEndpointStatus,
        now: Instant,
    ) -> bool {
        let Some(state) = self.profiles.get_mut(endpoint_id) else {
            return false;
        };
        if state.generation != Some(generation) {
            return false;
        };
        state.in_flight = false;
        match status {
            ClientEndpointStatus::Online => {
                state.attempts = 0;
                state.next_attempt = None;
            }
            ClientEndpointStatus::Attention | ClientEndpointStatus::Disabled => {
                state.next_attempt = None;
            }
            ClientEndpointStatus::Connecting | ClientEndpointStatus::Reconnecting => {
                state.attempts = state.attempts.saturating_add(1);
                state.next_attempt = Some(now + retry_delay(state.attempts));
            }
        }
        true
    }

    pub(crate) fn disconnected(&mut self, endpoint_id: &ClientEndpointId, now: Instant) {
        let Some(state) = self.profiles.get_mut(endpoint_id) else {
            return;
        };
        state.in_flight = false;
        state.attempts = state.attempts.saturating_add(1).max(1);
        state.next_attempt = Some(now + retry_delay(state.attempts));
    }
}

impl Drop for SshSupervisors {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

fn connect_once(
    profile: &super::SavedSshEndpoint,
    options: SshConnectOptions,
    endpoint_id: ClientEndpointId,
    generation: u64,
) -> Result<SshSupervisorEvent, (ClientEndpointId, std::io::Error)> {
    let mut connected =
        crate::remote::connect_saved_ssh(profile.id.as_str(), &profile.target, &profile.session)
            .map_err(|error| {
                let error = if crate::remote::saved_ssh_failure_needs_attention(&error) {
                    std::io::Error::new(
                        error.kind(),
                        format!(
                    "{error}. Run `{}` interactively to approve setup, then restart this client",
                    crate::remote::saved_ssh_bootstrap_command(&profile.target, &profile.session)
                ),
                    )
                } else {
                    error
                };
                (endpoint_id.clone(), error)
            })?;
    let handshake = super::super::do_handshake(
        &mut connected.stream,
        options.cols,
        options.rows,
        options.cell_width_px,
        options.cell_height_px,
        options.pixel_geometry_exact,
        Some(options.surface_size),
        options.endpoint_keybindings,
        options.mouse_capture,
        false,
    )
    .map_err(|error| (endpoint_id.clone(), handshake_error(error)))?;
    if handshake.encoding != RenderEncoding::SemanticFrame {
        return Err((
            endpoint_id,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSH endpoint did not negotiate the semantic client shell",
            ),
        ));
    }
    let negotiation = EndpointNegotiation::new(
        handshake.endpoint_methods.unwrap_or_default(),
        handshake.endpoint_capabilities.unwrap_or_default(),
    );
    if !negotiation.supports_surface_interest() || !negotiation.supports_health_check() {
        return Err((
            endpoint_id,
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!(
                    "{} does not support safe multi-machine lifecycle; run `herdr --remote {}` interactively to update it",
                    profile.label, profile.target
                ),
            ),
        ));
    }
    let reader = connected
        .stream
        .try_clone()
        .map_err(|error| (endpoint_id.clone(), error))?;
    Ok(SshSupervisorEvent::Connected {
        endpoint_id,
        generation,
        reader,
        writer: NativeEndpointTransport::with_lifetime(connected.stream, connected.bridge),
        negotiation,
    })
}

fn handshake_error(error: crate::client::ClientError) -> std::io::Error {
    use crate::client::ClientError;
    use crate::protocol::FramingError;

    match error {
        ClientError::ConnectionFailed(error) | ClientError::ConnectionLost(error) => error,
        ClientError::HandshakeRejected { error, .. } => {
            std::io::Error::new(std::io::ErrorKind::Unsupported, error)
        }
        ClientError::Protocol(FramingError::Io(error)) => error,
        ClientError::Protocol(error) => {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        }
        ClientError::ServerShutdown { reason } => std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            reason.unwrap_or_else(|| "remote server shut down during handshake".into()),
        ),
    }
}

fn retry_delay(attempt: u32) -> Duration {
    INITIAL_RETRY_DELAY
        .saturating_mul(
            1_u32
                .checked_shl(attempt.saturating_sub(1).min(6))
                .unwrap_or(u32::MAX),
        )
        .min(MAX_RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::endpoint::ProfileId;

    fn profile() -> super::super::SavedSshEndpoint {
        super::super::SavedSshEndpoint {
            id: ProfileId::parse("0123456789abcdef0123456789abcdef").unwrap(),
            label: "Build".into(),
            target: "build".into(),
            session: "agents".into(),
            enabled: true,
        }
    }

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(retry_delay(1), INITIAL_RETRY_DELAY);
        assert_eq!(retry_delay(100), MAX_RETRY_DELAY);
    }

    #[test]
    fn handshake_network_failures_retry_but_incompatibility_needs_attention() {
        let timeout = handshake_error(crate::client::ClientError::ConnectionLost(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"),
        ));
        assert_eq!(timeout.kind(), std::io::ErrorKind::TimedOut);
        assert!(!crate::remote::saved_ssh_failure_needs_attention(&timeout));

        let rejected = handshake_error(crate::client::ClientError::HandshakeRejected {
            version: 1,
            error: "surface capability missing".into(),
        });
        assert_eq!(rejected.kind(), std::io::ErrorKind::Unsupported);
        assert!(crate::remote::saved_ssh_failure_needs_attention(&rejected));
    }

    #[test]
    fn attention_stops_background_retries() {
        let now = Instant::now();
        let mut supervisors = SshSupervisors::new(&[profile()], now);
        let endpoint_id = ClientEndpointId::Ssh(profile().id);
        let generation = 2;
        supervisors
            .profiles
            .get_mut(&endpoint_id)
            .unwrap()
            .generation = Some(generation);
        supervisors.record_status(
            &endpoint_id,
            generation,
            ClientEndpointStatus::Attention,
            now,
        );
        let state = supervisors.profiles.get(&endpoint_id).unwrap();
        assert!(state.next_attempt.is_none());
        assert!(!state.in_flight);
    }

    #[test]
    fn stale_generation_status_does_not_change_reconnect_state() {
        let now = Instant::now();
        let mut supervisors = SshSupervisors::new(&[profile()], now);
        let endpoint_id = ClientEndpointId::Ssh(profile().id);
        let state = supervisors.profiles.get_mut(&endpoint_id).unwrap();
        state.generation = Some(4);
        state.in_flight = true;

        assert!(!supervisors.record_status(&endpoint_id, 3, ClientEndpointStatus::Attention, now));
        let state = supervisors.profiles.get(&endpoint_id).unwrap();
        assert!(state.in_flight);
        assert_eq!(state.generation, Some(4));
    }

    #[test]
    fn disconnect_schedules_a_bounded_noninteractive_retry() {
        let now = Instant::now();
        let mut supervisors = SshSupervisors::new(&[profile()], now);
        let endpoint_id = ClientEndpointId::Ssh(profile().id);
        supervisors.disconnected(&endpoint_id, now);
        let state = supervisors.profiles.get(&endpoint_id).unwrap();
        assert_eq!(state.next_attempt, Some(now + INITIAL_RETRY_DELAY));
    }
}
